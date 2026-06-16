"""
FastAPI routes for Helix Web Chat interface.

Endpoints:
- WebSocket /ws/chat/{session_id}
- GET /api/agents - List all agents
- GET /api/ucf - Get current UCF state
- POST /api/cycle - Trigger a cycle
"""

import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, WebSocketException
from pydantic import BaseModel

from apps.backend.core.exceptions import LLMProviderUnavailable
from apps.backend.core.unified_auth import get_current_user
from apps.backend.llm_agent_engine import LLMAgentEngine, initialize_llm_engine
from apps.backend.web_chat_server import AGENT_PERSONALITIES, connection_manager


async def execute_cycle_with_monitoring(steps: int = 36) -> dict:
    """Execute a coordination cycle with monitoring.

    Tries the real coordination integration first, then returns an honest
    unavailable state when the cycle engine cannot run.
    """
    try:
        from apps.backend.coordination.coordination_integration import trigger_coordination_cycle

        cycle_type_map = {108: "full_cycle", 12: "quick", 36: "daily"}
        cycle_type = cycle_type_map.get(steps, "daily")
        result = await trigger_coordination_cycle(cycle_type=cycle_type, parameters={"steps": steps})
        if isinstance(result, dict):
            return result
    except Exception as exc:
        logger.warning("Coordination cycle engine unavailable: %s", exc)

    return {
        "steps": steps,
        "success": False,
        "status": "unavailable",
        "message": "Coordination cycle engine is unavailable",
        "_default": True,
        "degraded_reason": "coordination_engine_unavailable",
        "timestamp": datetime.now(UTC).isoformat(),
    }


# Auth import — fail closed if unavailable
try:
    from apps.backend.core.auth import AuthManager

    _auth_available = True
except ImportError:
    _auth_available = False

logger = logging.getLogger(__name__)

router = APIRouter()

# Global LLM engine instance
llm_engine: LLMAgentEngine | None = None


# Initialize LLM engine on first use (lazy initialization)
async def get_llm_engine():
    """Get or initialize the LLM engine."""
    global llm_engine
    if llm_engine is None:
        try:
            llm_engine = await initialize_llm_engine()
            logger.info("✅ LLM engine initialized for web chat")
        except Exception as e:
            logger.warning("⚠️ LLM engine initialization failed: %s", e)
    return llm_engine


def get_fallback_response(agent_id: str) -> str:
    """Get a clear outage message for legacy compatibility."""
    return "[UNAVAILABLE] Helix is temporarily unavailable. Please try again in a moment."


# ============================================================================
# PYDANTIC MODELS
# ============================================================================


class CycleRequest(BaseModel):
    cycle_type: str = "daily"
    parameters: dict | None = None


class AgentChatRequest(BaseModel):
    agent_id: str
    message: str
    session_id: str | None = None


# ============================================================================
# WEBSOCKET ENDPOINT
# ============================================================================


@router.websocket("/ws/chat/{session_id}")
async def websocket_chat_endpoint(
    websocket: WebSocket,
    session_id: str,
    username: str = "Anonymous",
    token: str = Query(None),
):
    """
    WebSocket endpoint for real-time chat.

    Clients connect with session_id and authentication via:
    - Authorization header: "Bearer <jwt_token>"
    - httpOnly cookie: helix_auth_token
    Do NOT pass tokens in query params (they are logged by proxies)
    """
    # Validate JWT token before accepting connection
    if _auth_available:
        # Check Authorization header first (preferred), then fallback to httpOnly cookie
        auth_header = websocket.headers.get("authorization", "")
        cookie_token = websocket.cookies.get("helix_auth_token")

        # Extract Bearer token from header if present
        effective_token = None
        if auth_header.startswith("Bearer "):
            effective_token = auth_header[7:]  # Remove "Bearer " prefix
        elif auth_header:
            effective_token = auth_header
        elif cookie_token:
            effective_token = cookie_token

        # Legacy: also check query param for backward compatibility but log deprecation warning
        if token:
            logger.warning("⚠️ Token passed in query param (deprecated). Use Authorization header instead.")
            if not effective_token:
                effective_token = token

        if not effective_token:
            raise WebSocketException(code=1008, reason="Authentication required - missing token")
        try:
            AuthManager.verify_token(effective_token)
        except Exception:
            raise WebSocketException(code=1008, reason="Invalid or expired token") from None
    else:
        logger.error("Auth module unavailable — rejecting chat WS connection")
        raise WebSocketException(code=1013, reason="Authentication service unavailable")

    # Sanitize username: strip HTML/control chars, limit length
    import re as _re

    username = _re.sub(r"[<>&\"'\\]", "", username)[:50].strip() or "Anonymous"

    await connection_manager.connect(websocket, session_id, username)

    # Message size limit (1MB) to prevent memory exhaustion DoS
    MAX_MESSAGE_SIZE = 1024 * 1024

    try:
        while True:
            # Receive message from client
            data = await websocket.receive_json()

            # Validate message size
            if len(str(data)) > MAX_MESSAGE_SIZE:
                await websocket.send_json(
                    {
                        "error": "Message too large",
                        "detail": "Maximum message size is 1MB".format(),
                    }
                )
                continue

            # Handle the message
            await connection_manager.handle_message(session_id, data)

    except WebSocketDisconnect:
        connection_manager.disconnect(session_id)
        logger.info("Client %s disconnected", session_id)
    except Exception as e:
        logger.error("WebSocket error for %s: %s", session_id, e, exc_info=True)
        connection_manager.disconnect(session_id)


# ============================================================================
# REST API ENDPOINTS
# ============================================================================


@router.get("/api/chat/agents")
async def list_agents():
    """Get list of all available agents with their personalities."""
    agents = []
    for agent_id, agent_data in AGENT_PERSONALITIES.items():
        agents.append(
            {
                "id": agent_id,
                "name": agent_data["name"],
                "emoji": agent_data["emoji"],
                "color": agent_data["color"],
                "personality": agent_data["personality"],
                "greeting": agent_data["greeting"],
            }
        )

    return {
        "agents": agents,
        "total": len(agents),
        "online_users": len(connection_manager.active_connections),
    }


@router.get("/api/chat/agents/{agent_id}")
async def get_agent_details(agent_id: str):
    """Get detailed information about a specific agent."""
    if agent_id not in AGENT_PERSONALITIES:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

    agent_data = AGENT_PERSONALITIES[agent_id]

    # Derive coordination data from live UCF calculator
    def _get_agent_coordination(aid: str) -> dict:
        """Get per-agent coordination from UCF calculator with stable offset."""
        try:
            from apps.backend.services.ucf_calculator import UCFCalculator

            calc = UCFCalculator()
            ucf = calc.get_state()
            # Per-agent stable offset so agents differ slightly
            seed = hash(aid) % 1000
            offset = ((seed % 20) - 10) / 100.0
            return {
                "coordination": max(0.0, min(1.0, ucf.get("harmony", 0.5) + offset)),
                "throughput": max(0.0, min(1.0, ucf.get("throughput", 0.5) + offset)),
                "harmony": max(0.0, min(1.0, ucf.get("harmony", 0.5) + offset * 0.5)),
                "resilience": max(0.0, min(2.0, ucf.get("resilience", 1.0) + offset)),
                "friction": max(0.0, min(1.0, ucf.get("friction", 0.15) - offset)),
            }
        except Exception as exc:
            logger.warning("UCF metric calculation failed for agent %s: %s", aid, exc)
            return {
                "coordination": 0,
                "throughput": 0,
                "harmony": 0,
                "resilience": 0,
                "friction": 0,
                "_default": True,
                "degraded_reason": "ucf_calculation_failed",
            }

    agent_coordination = _get_agent_coordination(agent_id)

    return {
        "id": agent_id,
        "name": agent_data["name"],
        "emoji": agent_data["emoji"],
        "color": agent_data["color"],
        "personality": agent_data["personality"],
        "greeting": agent_data["greeting"],
        "coordination": agent_coordination,
        "online": True,
        "last_active": datetime.now(UTC).isoformat(),
        "message_count": 0,
        "specializations": (
            agent_data["personality"].split(" - ")[0]
            if " - " in agent_data["personality"]
            else agent_data["personality"]
        ),
    }


@router.get("/api/ucf")
async def get_ucf_state():
    """Get current UCF (Unified Coordination Field) state."""
    import json
    from pathlib import Path

    ucf_file = Path("Helix/state/ucf_state.json")

    if ucf_file.exists():
        try:
            with open(ucf_file, encoding="utf-8") as f:
                ucf_data = json.load(f)
            return {
                "coherence": ucf_data.get("harmony", 0.5) * 100,
                "entropy": 1 - ucf_data.get("harmony", 0.5),
                "performance_score": int(ucf_data.get("velocity", 1.0) * 14),
                "throughput": ucf_data.get("throughput", 0.5),
                "focus": ucf_data.get("focus", 0.5),
                "resilience": ucf_data.get("resilience", 0.5),
                "friction": ucf_data.get("friction", 0.1),
                "active_agents": 0,
                "cycles_completed_today": 0,
                "last_cycle": datetime.now(UTC).isoformat(),
                "timestamp": datetime.now(UTC).isoformat(),
                "status": "optimal" if ucf_data.get("harmony", 0) > 0.7 else "nominal",
                "source": "live",
            }
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Failed to load UCF state: %s", e)

    # Fallback to default values
    return {
        "coherence": 0,
        "entropy": 0,
        "performance_score": 0,
        "active_agents": 0,
        "cycles_completed_today": 0,
        "last_cycle": None,
        "timestamp": datetime.now(UTC).isoformat(),
        "status": "unavailable",
        "source": "default",
        "_default": True,
        "degraded_reason": "coordination_engine_unavailable",
    }


@router.get("/api/agent-swarm/coordination")
async def get_agent_coordination():
    """Get coordination metrics for all agents in the swarm."""
    # Load actual UCF state for base metrics
    try:
        from apps.backend.coordination.ucf_state_loader import load_ucf_state

        ucf = load_ucf_state()
        base_harmony = ucf.get("harmony", 0.0)
        base_throughput = ucf.get("throughput", 0.0)
        base_resilience = ucf.get("resilience", 0.0)
        base_friction = ucf.get("friction", 0.0)
    except Exception as e:
        logger.warning("UCF metrics unavailable, using defaults: %s", e)
        base_harmony = 0.0
        base_throughput = 0.0
        base_resilience = 0.0
        base_friction = 0.0

    # Agent definitions with their roles and emotional profiles
    agent_defs = {
        "kael": {"emotion": "focused", "mood": "orchestrating"},
        "lumina": {"emotion": "empathetic", "mood": "nurturing"},
        "vega": {"emotion": "innovative", "mood": "building"},
        "nova": {"emotion": "analytical", "mood": "observing"},
        "orion": {"emotion": "organized", "mood": "harmonizing"},
        "sage": {"emotion": "wise", "mood": "contemplating"},
        "nyx": {"emotion": "mysterious", "mood": "exploring"},
        "atlas": {"emotion": "inclusive", "mood": "connecting"},
        "oracle": {"emotion": "prescient", "mood": "foreseeing"},
        "agni": {"emotion": "passionate", "mood": "transforming"},
        "shadow": {"emotion": "vigilant", "mood": "protecting"},
        "phoenix": {"emotion": "hopeful", "mood": "renewing"},
        "echo": {"emotion": "receptive", "mood": "amplifying"},
        "praxis": {"emotion": "centered", "mood": "architecting"},
    }

    # Build coordination data from real UCF state
    agents_coordination = {}
    for agent_name, profile in agent_defs.items():
        agents_coordination[agent_name] = {
            "coordination": round(min(1.0, base_harmony * 1.1), 2),
            "throughput": round(min(1.0, base_throughput * 1.05), 2),
            "harmony": round(min(1.0, base_harmony), 2),
            "resilience": round(min(1.0, base_resilience * 0.95), 2),
            "friction": round(max(0.0, base_friction), 2),
            "emotion": profile["emotion"],
            "mood": profile["mood"],
        }

    # Detect zero-fallback and mark it
    _is_default = base_harmony == 0.0 and base_throughput == 0.0 and base_resilience == 0.0

    response = {
        "agents": agents_coordination,
        "swarm_coherence": round(base_harmony, 2),
        "active_connections": len(connection_manager.active_connections),
        "timestamp": datetime.now(UTC).isoformat(),
    }
    if _is_default:
        response["_default"] = True

    return response


@router.post("/api/cycle")
async def trigger_cycle(request: CycleRequest):
    """Trigger a cycle execution."""
    # Integrate with Coordination Cycle engine

    cycle_type = request.cycle_type

    try:
        # Execute the cycle based on type
        if cycle_type == "full_cycle":
            steps = 108  # Full cycle cycle
        elif cycle_type == "quick":
            steps = 12  # Quick cycle
        else:
            steps = 36  # Default cycle

        # Execute the cycle
        cycle_result = await execute_cycle_with_monitoring(steps=steps)

        if cycle_result.get("success") is False:
            await connection_manager.broadcast(
                {
                    "type": "cycle_unavailable",
                    "cycle_type": cycle_type,
                    "steps": steps,
                    "triggered_by": "web_api",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "result": cycle_result,
                }
            )
            raise HTTPException(
                status_code=503,
                detail=cycle_result.get("message", "Coordination cycle engine is unavailable"),
            )

        # Broadcast to all connected clients
        await connection_manager.broadcast(
            {
                "type": "cycle_triggered",
                "cycle_type": cycle_type,
                "steps": steps,
                "triggered_by": "web_api",
                "timestamp": datetime.now(UTC).isoformat(),
                "result": cycle_result,
            }
        )

        return {
            "success": True,
            "cycle_type": cycle_type,
            "steps": steps,
            "message": f"Cycle '{cycle_type}' completed successfully",
            "result": cycle_result,
            "timestamp": datetime.now(UTC).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error executing cycle: %s", e)
        raise HTTPException(status_code=500, detail="Failed to execute cycle") from e


@router.get("/api/stats")
async def get_web_chat_stats(current_user: dict = Depends(get_current_user)):
    """Get web chat statistics (authenticated)."""
    return {
        "active_connections": len(connection_manager.active_connections),
        "total_sessions": len(connection_manager.user_sessions),
    }


@router.post("/api/chat")
async def send_chat_message(request: AgentChatRequest):
    """Send a message to a specific agent."""
    agent_id = request.agent_id

    # Validate agent exists
    if agent_id not in AGENT_PERSONALITIES:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

    # Get agent personality
    agent_data = AGENT_PERSONALITIES[agent_id]

    # Generate intelligent agent responses using LLM system with fallback
    # ✅ INTEGRATED: Now uses LLM engine for dynamic, personality-driven responses
    llm_engine_instance = await get_llm_engine()
    if not llm_engine_instance:
        raise LLMProviderUnavailable(
            message="Helix LLM is temporarily unavailable",
            details={"agent_id": agent_id},
        ).to_http_exception()

    try:
        # Use LLM for intelligent response generation
        result = await llm_engine_instance.generate_agent_response(
            agent_id=agent_id,
            user_message=request.message,
            session_id=request.session_id or "default",
            context={"timestamp": datetime.now(UTC).isoformat()},
            allow_static_fallback=False,
        )
        # Unpack tuple (response_text, search_sources)
        if isinstance(result, tuple):
            response_text = result[0]
        else:
            response_text = result
    except LLMProviderUnavailable as exc:
        logger.warning("LLM generation unavailable for %s: %s", agent_id, exc)
        raise exc.to_http_exception() from exc
    except Exception as e:
        logger.warning("LLM generation failed for %s: %s", agent_id, e)
        raise LLMProviderUnavailable(
            message="Helix LLM is temporarily unavailable",
            details={"agent_id": agent_id, "error": type(e).__name__},
        ).to_http_exception() from e

    # Load real UCF coordination level
    try:
        from apps.backend.coordination.ucf_state_loader import get_ucf_metrics

        _ucf = get_ucf_metrics()
        _coordination = _ucf.get("performance_score", 0)
    except Exception as e:
        logger.debug("UCF coordination score unavailable: %s", e)
        _coordination = 0

    return {
        "agent_id": agent_id,
        "agent_name": agent_data["name"],
        "response": response_text,
        "performance_score": _coordination,
        "timestamp": datetime.now(UTC).isoformat(),
        "emotion": "engaged",
    }


@router.get("/api/session/new")
async def create_new_session(username: str | None = "Anonymous"):
    """Create a new chat session ID."""
    session_id = str(uuid.uuid4())

    return {
        "session_id": session_id,
        "username": username,
        "websocket_url": f"/ws/chat/{session_id}?username={username}",
    }

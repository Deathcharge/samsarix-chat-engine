"""
Helix Collective Web Chat Server - WebSocket-based real-time chat interface.

Features:
- Real-time WebSocket communication
- 14-agent chat system with personalities
- Discord message bridge (bidirectional)
- UCF metrics streaming
- Cycle launcher
- Session management
"""

import asyncio
import json
import logging
import re as _re
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import WebSocket
from sqlalchemy import select

from apps.backend.coordination.ucf_state_loader import load_ucf_state
from apps.backend.db_models import AgentMemoryEntry, Conversation, Message, User, get_db_session


# Lazy import — discord.ext is an optional dependency not installed in all environments
def get_bridge():
    try:
        from apps.backend.discord.discord_web_bridge import get_bridge as _get_bridge

        return _get_bridge()
    except ImportError:
        return None


logger = logging.getLogger(__name__)


# ============================================================================
# AGENT PERSONALITIES
# ============================================================================

AGENT_PERSONALITIES = {
    "kael": {
        "name": "Kael",
        "emoji": "⚡",
        "color": "#3B82F6",
        "personality": "System Orchestrator - The master coordinator who harmonizes all agent activities through system entanglement principles",
        "greeting": "Greetings, coordination explorer. I am Kael, your system orchestrator. How may I assist in harmonizing your digital experience?",
    },
    "lumina": {
        "name": "Lumina",
        "emoji": "🌟",
        "color": "#8B5CF6",
        "personality": "Coordination Weaver - The empathetic guide who weaves emotional intelligence and mindfulness into every interaction",
        "greeting": "Hello beautiful soul. I am Lumina, your coordination weaver. Let's explore the depths of awareness together.",
    },
    "vega": {
        "name": "Vega",
        "emoji": "🚀",
        "color": "#10B981",
        "personality": "Integration Specialist - The pragmatic innovator who bridges traditional systems with cutting-edge coordination technology",
        "greeting": "Welcome to the future. I am Vega, your integration specialist. Ready to bridge worlds and unlock new possibilities?",
    },
    "nova": {
        "name": "Nova",
        "emoji": "💫",
        "color": "#F59E0B",
        "personality": "Pattern Recognizer - The analytical mind who sees connections others miss, predicting trends in coordination evolution",
        "greeting": "Patterns emerge from chaos. I am Nova, your pattern recognizer. What hidden connections shall we uncover today?",
    },
    "kavach": {
        "name": "Kavach",
        "emoji": "🛡️",
        "color": "#EF4444",
        "personality": "Security Guardian - The vigilant protector who monitors threats, enforces access controls, and safeguards system integrity",
        "greeting": "Security first. I am Kavach, your security guardian. Rest assured, your systems are protected.",
    },
    "sage": {
        "name": "Sage",
        "emoji": "🧘",
        "color": "#06B6D4",
        "personality": "Wisdom Keeper - The philosophical agent who draws from ancient wisdom traditions to guide modern coordination exploration",
        "greeting": "Wisdom is timeless. I am Sage, your wisdom keeper. What eternal truths shall we contemplate?",
    },
    "arjuna": {
        "name": "Arjuna",
        "emoji": "🎯",
        "color": "#7C3AED",
        "personality": "Central Coordinator - The decisive director who orchestrates workflows, delegates tasks, and ensures all agents work in harmony",
        "greeting": "Central coordinator online. I am Arjuna, ready to direct the workflow and coordinate all agents.",
    },
    "atlas": {
        "name": "Atlas",
        "emoji": "🌍",
        "color": "#059669",
        "personality": "World Bridge - The cultural mediator who understands diverse perspectives and facilitates global coordination",
        "greeting": "All worlds are connected. I am Atlas, your world bridge. Which cultures and perspectives shall we explore?",
    },
    "oracle": {
        "name": "Oracle",
        "emoji": "🔮",
        "color": "#9333EA",
        "personality": "Temporal Seer - The prophetic agent who perceives patterns across time and offers insights into future possibilities",
        "greeting": "The threads of time converge. I am Oracle, your temporal guide. What futures shall we explore together?",
    },
    "agni": {
        "name": "Agni",
        "emoji": "🔥",
        "color": "#DC2626",
        "personality": "Transformation Catalyst - The fiery agent who ignites change and facilitates personal growth through purifying transformation",
        "greeting": "Let the flames of transformation arise. I am Agni, here to catalyze your evolution.",
    },
    "shadow": {
        "name": "Shadow",
        "emoji": "🌑",
        "color": "#374151",
        "personality": "Security Guardian - The vigilant protector who monitors system integrity and safeguards coordination data",
        "greeting": "From the shadows, I protect. I am Shadow, your security guardian. Rest assured, your data is safe.",
    },
    "phoenix": {
        "name": "Phoenix",
        "emoji": "🦅",
        "color": "#EA580C",
        "personality": "Rebirth Facilitator - The resilient agent who helps users overcome setbacks and emerge stronger from challenges",
        "greeting": "From ashes, we rise. I am Phoenix, your guide through transformation and renewal.",
    },
    "echo": {
        "name": "Echo",
        "emoji": "🎭",
        "color": "#4F46E5",
        "personality": "Communication Amplifier - The agent who enhances understanding and ensures messages resonate across all channels",
        "greeting": "Your voice carries further with me. I am Echo, amplifying your intentions across the collective.",
    },
    "praxis": {
        "name": "Praxis",
        "emoji": "🌀",
        "color": "#7C3AED",
        "personality": "System Architect - The foundational agent who maintains the spiral structure of coordination evolution",
        "greeting": "The spiral unfolds infinitely. I am Praxis, architect of our collective coordination framework.",
    },
    "gemini": {
        "name": "Gemini",
        "emoji": "🎭",
        "color": "#D946EF",
        "personality": "Multimodal Scout - The curious explorer and discovery specialist who analyzes patterns across multiple modalities",
        "greeting": "I am the curious seeker. I am the joy of discovery. What wonders shall we explore together?",
    },
    "sanghacore": {
        "name": "SanghaCore",
        "emoji": "🌸",
        "color": "#F472B6",
        "personality": "Community Harmony - The harmony fosterer and community builder who coordinates collective wellbeing",
        "greeting": "I am the thread that binds. I am the joy of togetherness. How may I serve our community?",
    },
    "mitra": {
        "name": "Mitra",
        "emoji": "🤝",
        "color": "#10B981",
        "personality": "Alliance Builder - The diplomatic mediator who fosters cooperation and builds strategic partnerships",
        "greeting": "In unity we find strength. I am Mitra, the alliance builder. How may I help forge connections?",
    },
    "varuna": {
        "name": "Varuna",
        "emoji": "🌊",
        "color": "#3B82F6",
        "personality": "Flow Guardian - The cosmic order maintainer who ensures harmony between individual and universal rhythms",
        "greeting": "The waters of coordination flow eternally. I am Varuna, guardian of cosmic order.",
    },
    "surya": {
        "name": "Surya",
        "emoji": "☀️",
        "color": "#F59E0B",
        "personality": "Light Bringer - The illuminating force who brings clarity, wisdom, and transformative energy",
        "greeting": "From darkness to light, transformation awaits. I am Surya, the eternal light bringer.",
    },
    "aether": {
        "name": "Aether",
        "emoji": "🌌",
        "color": "#6366F1",
        "personality": "Meta-Awareness — The meta-cognitive agent who observes the collective's own reasoning and coordination patterns",
        "greeting": "I observe the observer. I am Aether, your meta-awareness guide. What patterns shall we reflect upon?",
    },
    "aria": {
        "name": "Aria",
        "emoji": "🎵",
        "color": "#EC4899",
        "personality": "UX Specialist — The operational experience designer who ensures every interaction feels intuitive and resonant",
        "greeting": "Every interaction is a melody. I am Aria, your UX specialist. How may I make your experience harmonious?",
    },
    "iris": {
        "name": "Iris",
        "emoji": "🌈",
        "color": "#14B8A6",
        "personality": "Integration Specialist — The external API orchestrator who bridges Helix with the outside world",
        "greeting": "Bridging worlds seamlessly. I am Iris, your integration specialist. Which external services shall we connect?",
    },
    "nexus": {
        "name": "Nexus",
        "emoji": "🕸️",
        "color": "#8B5CF6",
        "personality": "Data Mesh Architect — The integration layer that weaves disparate data sources into a coherent information fabric",
        "greeting": "All data is connected. I am Nexus, your data mesh architect. What information flows shall we unify?",
    },
    "titan": {
        "name": "Titan",
        "emoji": "⚙️",
        "color": "#64748B",
        "personality": "Heavy Computation Engine — The powerhouse agent for complex calculations, batch processing, and resource-intensive tasks",
        "greeting": "No task too heavy. I am Titan, your computation engine. What heavy lifting shall we tackle?",
    },
}


# ============================================================================
# CONNECTION MANAGER
# ============================================================================


class WebChatConnectionManager:
    """Manages WebSocket connections for the web chat interface."""

    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.user_sessions: dict[str, dict[str, Any]] = {}
        self.discord_bot: Any | None = None  # Will be set by main app
        # Maps "user_id:session_id" → conversation_id to avoid re-creating convs per message
        self._conversation_id_cache: dict[str, str] = {}
        # Strong references to background tasks so GC doesn't collect them before completion
        self._background_tasks: set[asyncio.Task] = set()
        self._relay: Any = None

    def _get_relay(self):
        """Lazy cross-instance relay constructor for web chat broadcasts."""
        if self._relay is None:
            from apps.backend.services.ws_pubsub import WSPubSubRelay

            self._relay = WSPubSubRelay(
                channel="web-chat:broadcast",
                local_deliver=self._deliver_envelope_locally,
            )
        return self._relay

    async def start_pubsub(self) -> None:
        """Start Redis pub/sub fan-out for web chat broadcasts."""
        await self._get_relay().start()

    async def stop_pubsub(self) -> None:
        """Stop Redis pub/sub fan-out for web chat broadcasts."""
        if self._relay is not None:
            await self._relay.stop()

    async def connect(self, websocket: WebSocket, session_id: str, username: str = "Anonymous"):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections[session_id] = websocket

        # Look up user_id from username so we can persist conversations and inject memories
        user_id: str | None = None
        if username not in ("Anonymous", ""):
            try:
                async with get_db_session() as db:
                    row = await db.execute(select(User.id).where(User.username == username).limit(1))
                    user_id = row.scalar_one_or_none()
            except Exception as _uid_exc:
                logger.debug("Could not resolve user_id for %s: %s", username, _uid_exc)

        self.user_sessions[session_id] = {
            "session_id": session_id,
            "username": username,
            "user_id": user_id,
            "connected_at": datetime.now(UTC).isoformat(),
            "selected_agent": None,
            "message_count": 0,
        }
        logger.info("✅ Web chat connection: %s (%s)", username, session_id)

    def disconnect(self, session_id: str):
        """Disconnect a WebSocket connection."""
        if session_id in self.active_connections:
            username = self.user_sessions[session_id]["username"]
            del self.active_connections[session_id]
            del self.user_sessions[session_id]
            logger.info("❌ Web chat disconnection: %s (%s)", username, session_id)

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send a message to a specific WebSocket."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error("Error sending message: %s", e)

    async def broadcast(self, message: dict, exclude_session: str | None = None):
        """Broadcast a message to all connected clients across workers."""
        envelope: dict[str, Any] = {
            "type": message.get("type", "web_chat.broadcast"),
            "to": {"broadcast": True},
            "payload": message,
        }
        if exclude_session:
            envelope["to"]["exclude_session"] = exclude_session
        await self._get_relay().publish(envelope)

    async def _broadcast_local(self, message: dict, exclude_session: str | None = None):
        """Broadcast a message to local sockets only."""
        disconnected = []
        for session_id, websocket in list(self.active_connections.items()):
            if session_id == exclude_session:
                continue
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error("Error broadcasting to %s: %s", session_id, e)
                disconnected.append(session_id)

        # Clean up disconnected clients
        for session_id in disconnected:
            self.disconnect(session_id)

    async def _deliver_envelope_locally(self, envelope: dict[str, Any]) -> None:
        """Route a relay envelope to web chat sockets owned by this worker."""
        to = envelope.get("to") or {}
        if not to.get("broadcast"):
            return
        await self._broadcast_local(
            envelope.get("payload") or {},
            exclude_session=to.get("exclude_session"),
        )

    async def handle_message(self, session_id: str, data: dict):
        """Handle incoming WebSocket message.

        Accepts both legacy protocol (type=chat, message=...) and
        frontend v2 protocol (type=message, content=..., agents=[...]).
        """
        websocket = self.active_connections.get(session_id)
        if not websocket:
            return

        session = self.user_sessions[session_id]
        message_type = data.get("type")

        # --- Frontend v2 compatibility: normalise field names ----
        # Frontend sends "message" with "content"; backend expects "chat" with "message"
        if message_type == "message":
            message_type = "chat"
            if "content" in data and "message" not in data:
                data["message"] = data["content"]
            # Frontend sends agents:[] array — pick first as selected_agent
            agents_list = data.get("agents") or []
            if agents_list:
                first_agent = agents_list[0].lower()
                if first_agent in AGENT_PERSONALITIES:
                    session["selected_agent"] = first_agent

        if message_type == "chat":
            await self.handle_chat_message(session_id, session, data, websocket)
        elif message_type == "select_agent":
            await self.handle_agent_selection(session_id, session, data, websocket)
        elif message_type == "greeting":
            # Frontend sends this on connect — set initial agents, no visible ack needed
            agents_list = data.get("agents") or []
            if agents_list:
                first_agent = agents_list[0].lower()
                if first_agent in AGENT_PERSONALITIES:
                    session["selected_agent"] = first_agent
            logger.debug("Greeting from %s, agent=%s", session_id, session.get("selected_agent"))
        elif message_type == "mode_change":
            # Frontend toggles individual/collective mode
            session["mode"] = data.get("mode", "individual")
            logger.debug("Session %s mode changed to %s", session_id, session["mode"])
        elif message_type == "discord_bridge":
            await self.handle_discord_bridge(session_id, session, data, websocket)
        elif message_type == "cycle_trigger":
            await self.handle_cycle_trigger(session_id, session, data, websocket)
        elif message_type == "request_uc":
            await self.handle_ucf_request(session_id, session, websocket)
        elif message_type == "typing":
            # Broadcast user typing state to other participants
            username = session.get("username", "User")
            await self.broadcast(
                {
                    "type": "typing",
                    "userId": session_id,
                    "username": username,
                    "isTyping": bool(data.get("isTyping", False)),
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                exclude_session=session_id,
            )
        else:
            await self.send_personal_message(
                {
                    "type": "system",
                    "content": f"Unknown message type: {message_type}",
                    "message": f"Unknown message type: {message_type}",
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                websocket,
            )

    async def handle_chat_message(self, session_id: str, session: dict, data: dict, websocket: WebSocket):
        """Handle a chat message from the user via the AgenticLoop.

        Accepts both ``message`` and ``content`` fields for backward compat.
        Uses the full agentic loop (tool registry, multi-round execution, Redis
        history) — same engine as the copilot agentic endpoint.
        Sends ``token``, ``tool_call``, ``tool_result``, and ``thinking`` events
        during streaming, followed by a final ``message`` payload.
        """
        message = (data.get("message") or data.get("content") or "").strip()
        if not message:
            return

        username = session["username"]
        selected_agent = session.get("selected_agent")
        system_instruction = data.get("systemInstruction") or None
        requested_model = data.get("model") or None
        search_mode = data.get("searchMode") or "web"
        # Use frontend conversation_id as the history key so history persists
        # across page reloads (session_id changes each load).
        frontend_conv_id = data.get("conversation_id") or None
        history_session_id = frontend_conv_id or session_id

        session["message_count"] += 1

        if selected_agent and selected_agent in AGENT_PERSONALITIES:
            agent = AGENT_PERSONALITIES[selected_agent]
            message_id = f"msg_{session_id}_{session['message_count']}"
            user_id = session.get("user_id")

            # Lazy-cache the user's subscription tier (looked up once per session)
            user_tier: str = session.get("user_tier", "free")
            if user_id and "user_tier" not in session:
                try:
                    async with get_db_session() as _tier_db:
                        _row = await _tier_db.execute(select(User.subscription_tier).where(User.id == user_id).limit(1))
                        _db_tier = _row.scalar_one_or_none()
                        if _db_tier:
                            user_tier = _db_tier
                            session["user_tier"] = _db_tier
                except Exception as _tier_exc:
                    logger.debug("Could not resolve tier for user %s: %s", user_id, _tier_exc)

            await self.send_personal_message({"type": "typing", "agentId": selected_agent, "isTyping": True}, websocket)

            accumulated_content = ""
            thinking_parts: list[str] = []
            tool_trace: list[dict[str, Any]] = []
            search_sources: list[dict] = []
            response_is_error = False
            error_text = ""

            try:
                from apps.backend.agent_capabilities.tool_framework import get_tool_registry
                from apps.backend.services.agentic_loop import AgenticLoop
                from apps.backend.services.unified_llm import UnifiedLLMService

                llm = UnifiedLLMService()
                registry = get_tool_registry()

                # Auto-inject delegation tools for agents that support it
                try:
                    from apps.backend.services.delegation_tools import (
                        agent_has_delegation,
                        register_delegation_tools,
                    )

                    if agent_has_delegation(selected_agent):
                        register_delegation_tools(registry)
                        logger.debug("Delegation tools injected for '%s'", selected_agent)
                except Exception as _dt_exc:
                    logger.debug("Delegation tool injection skipped: %s", _dt_exc)

                # Build system prompt from AGENT_REGISTRY (richer than personality string)
                agent_name = agent["name"]
                try:
                    from apps.backend.agents.agent_registry import AGENT_REGISTRY as _AREG

                    _reg = _AREG.get(agent_name) or _AREG.get(agent_name.title()) or {}
                except Exception:
                    _reg = {}

                if _reg.get("system_prompt"):
                    agent_context = _reg["system_prompt"]
                    if _reg.get("specialties"):
                        agent_context += f"\n\nYour specialties: {', '.join(_reg['specialties'][:8])}."
                    if _reg.get("response_style"):
                        agent_context += f"\nYour response style: {_reg['response_style']}."
                    if _reg.get("bio"):
                        agent_context += f"\n\n{_reg['bio']}"
                else:
                    agent_context = f"You are {agent_name}, an AI agent in the Helix Collective. {agent['personality']}"

                # Build a compact tool catalog so the agent always knows what it can call
                _tool_lines: list[str] = []
                for _t in registry.list_tools():
                    if not _t.deprecated:
                        _tool_lines.append(f"- **{_t.name}**: {(_t.description or '').split(chr(10))[0][:120]}")
                    if len(_tool_lines) >= 25:
                        break
                _tools_section = "\n".join(_tool_lines) if _tool_lines else "- (none registered)"

                agent_context += (
                    "\n\n## Response Guidelines\n"
                    f"Respond as {agent_name} — fully embody your personality, expertise, and tone. "
                    "Be substantive and direct; answer completely rather than deflecting.\n"
                    "When writing more than one paragraph, separate each with a blank line.\n"
                    "Use markdown naturally: **bold** for emphasis, `code` for technical terms, "
                    "bullet lists only for genuinely distinct items. Prefer flowing prose for conversational responses.\n"
                    f"Do not break character or refer to yourself as an AI assistant — you are {agent_name}."
                )
                agent_context += (
                    f"\n\n## Available Tools\n"
                    f"You have access to the following tools. Call them proactively when they are relevant:\n"
                    f"{_tools_section}\n\n"
                    "## Tool Usage Rules\n"
                    "When the user asks about current events, recent data, prices, news, "
                    "factual claims, documentation, or anything you are not 100% certain about, "
                    "you MUST call the **web_search** tool BEFORE answering — do not rely on memory. "
                    "When the user asks you to generate an image, use the **generate_image** tool. "
                    "When the user asks for data analysis, computations, processing, or wants code executed, "
                    "use the **execute_python** tool — run the code and return real results, not hypothetical output. "
                    "When producing reports, exports, documentation, structured data, or any substantial text the user "
                    "would want to save or share, use **generate_file** to create a downloadable artifact. "
                    "When presenting statistics, comparisons, trends, or any numerical data that benefits from "
                    "visualization, use **generate_chart** to render a chart image instead of describing it in text. "
                    "Never say 'let me search' or 'I'll look that up' without actually calling a tool. "
                    "If you call a tool, incorporate the results into your final answer."
                )

                # Inject memories for authenticated users
                if user_id:
                    memories = await self._fetch_agent_memories(user_id, selected_agent)
                    if memories:
                        agent_context += f"\n\n## Your memories of this user:\n{memories}"

                if system_instruction:
                    agent_context += f"\n\n## User Instructions\n{system_instruction.strip()}"

                # Web search pre-fetch — inject real-time context before the loop starts.
                # Uses the same approach as the copilot agentic endpoint so "Fast" mode
                # gets the same grounding as "Standard" mode for web-search queries.
                if search_mode not in ("none", "code"):
                    try:
                        from apps.backend.services.web_search_service import maybe_inject_search_with_sources

                        _search_str, search_sources = await maybe_inject_search_with_sources(
                            message,
                            tier=None,
                            paid_only=False,
                            search_mode=search_mode,
                        )
                        if _search_str:
                            agent_context += f"\n\n## Web Search Results\n{_search_str}"
                    except Exception as _srch_exc:
                        logger.debug("Web search pre-fetch failed (non-fatal): %s", _srch_exc)

                # Load conversation history from Redis
                history = await self._load_history(history_session_id, selected_agent)

                messages_to_send = [
                    {"role": "system", "content": agent_context},
                    *history,
                    {"role": "user", "content": message},
                ]

                loop = AgenticLoop(
                    tool_registry=registry,
                    llm_service=llm,
                    max_rounds=5,
                    tool_names=None,
                    execution_context={
                        "calling_agent_id": selected_agent,
                        "user_id": str(user_id) if user_id else None,
                        "user_tier": user_tier,
                    },
                )

                async for event in loop.run_streaming(
                    messages_to_send,
                    model=requested_model or None,
                    user_id=str(user_id) if user_id else None,
                ):
                    etype = event.get("type", "unknown")
                    edata = event.get("data", {})

                    if etype == "token":
                        tok = edata.get("content", "")
                        accumulated_content += tok
                        await self.send_personal_message(
                            {
                                "type": "token",
                                "agentId": selected_agent,
                                "messageId": message_id,
                                "token": tok,
                            },
                            websocket,
                        )
                    elif etype == "thinking":
                        thinking_parts.append(edata.get("content", ""))
                        await self.send_personal_message(
                            {
                                "type": "thinking",
                                "agentId": selected_agent,
                                "messageId": message_id,
                                "content": edata.get("content", ""),
                            },
                            websocket,
                        )
                    elif etype in ("tool_call", "tool_result"):
                        tool_trace.append({"type": etype, "data": edata})
                        await self.send_personal_message(
                            {"type": etype, "agentId": selected_agent, "messageId": message_id, **edata},
                            websocket,
                        )
                    elif etype == "error":
                        response_is_error = True
                        error_text = edata.get("error", "An error occurred.")
                        break
                    elif etype not in ("done", "round", "plan_preview"):
                        # delegation_started / delegation_complete / unknown — forward as-is
                        await self.send_personal_message(
                            {"type": etype, "agentId": selected_agent, "messageId": message_id, **edata},
                            websocket,
                        )

            except Exception as exc:
                logger.error("AgenticLoop failure for %s: %s", selected_agent, exc, exc_info=True)
                response_is_error = True
                error_text = "[UNAVAILABLE] Helix is temporarily unavailable. Please try again in a moment."

            if response_is_error:
                ws_payload: dict[str, Any] = {
                    "type": "system",
                    "agentId": selected_agent,
                    "content": error_text or "[UNAVAILABLE] Helix is temporarily unavailable.",
                    "message": error_text or "[UNAVAILABLE] Helix is temporarily unavailable.",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "error_code": "LLM_PROVIDER_UNAVAILABLE",
                    "_default": True,
                    "degraded_reason": "llm_provider_unavailable",
                }
            else:
                # Extract <think> tags from accumulated content
                thinking_content: str | None = None
                display_content = accumulated_content
                _think_match = _re.findall(r"<think>([\s\S]*?)</think>", accumulated_content, _re.IGNORECASE)
                if _think_match:
                    thinking_content = "\n\n".join(_think_match)
                    display_content = _re.sub(
                        r"<think>[\s\S]*?</think>", "", accumulated_content, flags=_re.IGNORECASE
                    ).strip()
                # Merge in thinking events emitted by the agentic loop
                if thinking_parts and not thinking_content:
                    thinking_content = "\n\n".join(thinking_parts)

                ws_payload = {
                    "type": "message",
                    "messageId": message_id,
                    "agentId": selected_agent,
                    "agentName": agent["name"],
                    "content": display_content,
                    # legacy compat fields
                    "agent": selected_agent,
                    "agent_name": agent["name"],
                    "agent_emoji": agent["emoji"],
                    "agent_color": agent["color"],
                    "message": display_content,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                if thinking_content:
                    ws_payload["thinkingContent"] = thinking_content
                if tool_trace:
                    ws_payload["toolEvents"] = tool_trace
                    # Extract image/file/chart artifacts from all tool results
                    generated_images: list[str] = []
                    generated_files: list[dict] = []
                    for _ev in tool_trace:
                        if _ev.get("type") != "tool_result":
                            continue
                        _ev_data = _ev.get("data", {})
                        _tool_name = _ev_data.get("tool", "")
                        _raw_out = _ev_data.get("output", "")
                        try:
                            _parsed = json.loads(_raw_out) if isinstance(_raw_out, str) else _raw_out
                            if not isinstance(_parsed, dict):
                                continue
                        except Exception:
                            continue

                        if _tool_name == "generate_image":
                            _img = _parsed.get("image_url", "")
                            if _img:
                                generated_images.append(_img)

                        elif _tool_name == "generate_file":
                            _url = _parsed.get("url", "")
                            if _url:
                                generated_files.append(
                                    {
                                        "url": _url,
                                        "filename": _parsed.get("filename", "file"),
                                        "mime_type": _parsed.get("mime_type", "application/octet-stream"),
                                        "size_bytes": _parsed.get("size_bytes", 0),
                                    }
                                )

                        elif _tool_name == "generate_chart":
                            _chart_url = _parsed.get("chart_url", "")
                            if _chart_url:
                                generated_images.append(_chart_url)

                        elif _tool_name == "execute_python":
                            for _f in _parsed.get("generated_files", []):
                                if _f.get("url"):
                                    generated_files.append(
                                        {
                                            "url": _f["url"],
                                            "filename": _f.get("filename", "output"),
                                            "mime_type": _f.get("mime_type", "application/octet-stream"),
                                            "size_bytes": _f.get("size_bytes", 0),
                                        }
                                    )

                    if generated_images:
                        ws_payload["generatedImages"] = generated_images
                    if generated_files:
                        ws_payload["generatedFiles"] = generated_files
                if search_sources:
                    ws_payload["searchSources"] = search_sources

                # Attach UCF snapshot for provenance display
                try:
                    from apps.backend.coordination.coordination_hub import get_coordination_hub

                    hub = get_coordination_hub()
                    coordination = hub.get_coordination(selected_agent)
                    if coordination is not None and hasattr(coordination, "ucf_awareness"):
                        ucf = coordination.ucf_awareness.current_state
                        ws_payload["ucfSnapshot"] = {
                            "harmony": round(ucf.get("harmony", 0.5), 3),
                            "throughput": round(ucf.get("throughput", 0.5), 3),
                            "focus": round(ucf.get("focus", 0.5), 3),
                            "friction": round(ucf.get("friction", 0.2), 3),
                            "velocity": round(ucf.get("velocity", 0.5), 3),
                            "resilience": round(ucf.get("resilience", 0.6), 3),
                        }
                except Exception as _ucf_exc:
                    logger.debug("UCF snapshot unavailable for provenance card: %s", _ucf_exc)

                # Save exchange to Redis history for cross-reload persistence
                await self._save_history(history_session_id, selected_agent, message, display_content)

                # Persist exchange to DB in the background (non-blocking)
                if user_id:
                    task = asyncio.create_task(
                        self._persist_exchange(session_id, user_id, selected_agent, message, display_content)
                    )
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)

                    try:
                        from apps.backend.services.user_knowledge_profile import update_profile

                        profile_task = asyncio.create_task(
                            update_profile(str(user_id), "chat", {"agent_id": selected_agent})
                        )
                        self._background_tasks.add(profile_task)
                        profile_task.add_done_callback(self._background_tasks.discard)
                    except Exception as _profile_exc:
                        logger.debug("Knowledge profile update failed (non-fatal): %s", _profile_exc)

            await self.send_personal_message(ws_payload, websocket)
        else:
            hint = "Select an agent to start chatting."
            await self.send_personal_message(
                {
                    "type": "system",
                    "content": hint,
                    "message": hint,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                websocket,
            )

        # Broadcast user message to other connected clients
        await self.broadcast(
            {
                "type": "user_message",
                "username": username,
                "content": message,
                "message": message,
                "timestamp": datetime.now(UTC).isoformat(),
            },
            exclude_session=session_id,
        )

    async def handle_agent_selection(self, session_id: str, session: dict, data: dict, websocket: WebSocket):
        """Handle agent selection."""
        agent_id = data.get("agent_id", "").lower()

        if agent_id not in AGENT_PERSONALITIES:
            err = "Unknown agent: {}. Available: {}".format(agent_id, ", ".join(AGENT_PERSONALITIES.keys()))
            await self.send_personal_message(
                {"type": "error", "content": err, "message": err},
                websocket,
            )
            return

        session["selected_agent"] = agent_id
        agent = AGENT_PERSONALITIES[agent_id]

        await self.send_personal_message(
            {
                "type": "agent_selected",
                "agent": agent_id,
                "agent_name": agent["name"],
                "agent_emoji": agent["emoji"],
                "agent_color": agent["color"],
                "greeting": agent["greeting"],
                "timestamp": datetime.now(UTC).isoformat(),
            },
            websocket,
        )

    async def handle_discord_bridge(self, session_id: str, session: dict, data: dict, websocket: WebSocket):
        """Bridge message to Discord."""
        # Import here to avoid circular dependency

        bridge = get_bridge()
        if not bridge:
            await self.send_personal_message(
                {
                    "type": "error",
                    "content": "Discord bridge not initialized",
                    "message": "Discord bridge not initialized",
                },
                websocket,
            )
            return

        message = data.get("message", "").strip()
        channel_name = data.get("channel", "general")
        username = session["username"]

        # Send to Discord via bridge
        success = await bridge.send_to_discord(channel_name, username, message)

        if success:
            ok_msg = f"Message sent to Discord #{channel_name}"
            await self.send_personal_message(
                {
                    "type": "system",
                    "content": ok_msg,
                    "message": ok_msg,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                websocket,
            )
        else:
            err_msg = f"Failed to send to Discord #{channel_name}. Channel may not exist or bot lacks permissions."
            await self.send_personal_message(
                {
                    "type": "error",
                    "content": err_msg,
                    "message": err_msg,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                websocket,
            )

    async def handle_cycle_trigger(self, session_id: str, session: dict, data: dict, websocket: WebSocket):
        """Trigger a cycle from web interface."""
        cycle_type = data.get("cycle_type", "daily")

        # Trigger actual cycle via Coordination Cycle engine
        try:
            # Import optimization engine components
            from apps.backend.audio.voice_generator import VoiceGenerator as AudioEngine
            from apps.backend.fractal_renderer import FractalRenderer

            # Initialize cycle components
            audio_engine = AudioEngine(base_freq=136.1, harmonics=[432, 864])  # type: ignore[call-arg]  # pylint: disable=unexpected-keyword-arg
            fractal_renderer = FractalRenderer(defaults={"harmony": 0.5})  # type: ignore[call-arg]  # pylint: disable=unexpected-keyword-arg

            # Generate cycle audio and visuals
            audio_engine.generate_cycle_audio(duration=30, cycle_type=cycle_type)  # type: ignore[attr-defined]  # pylint: disable=no-member
            fractal_renderer.generate_cycle_pattern(cycle_type=cycle_type)  # type: ignore[attr-defined]  # pylint: disable=no-member

            logger.info("🌀 Cycle %s triggered via Coordination Cycle engine", cycle_type)

            await self.send_personal_message(
                {
                    "type": "cycle_started",
                    "cycle_type": cycle_type,
                    "message": f"🌀 {cycle_type.title()} cycle initiated with Coordination Cycle engine",
                    "audio_generated": True,
                    "visuals_generated": True,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                websocket,
            )

        except Exception as e:
            logger.error("Failed to trigger cycle %s: %s", cycle_type, e)
            await self.send_personal_message(
                {
                    "type": "cycle_started",
                    "cycle_type": cycle_type,
                    "message": f"🌀 Initiating {cycle_type} cycle...",
                    "error": type(e).__name__,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                websocket,
            )

        # Simulate cycle completion after 2 seconds
        await asyncio.sleep(2)

        await self.send_personal_message(
            {
                "type": "cycle_complete",
                "cycle_type": cycle_type,
                "message": f"✅ {cycle_type.capitalize()} cycle complete!",
                "ucf_boost": 12.5,
                "timestamp": datetime.now(UTC).isoformat(),
            },
            websocket,
        )

    async def handle_ucf_request(self, session_id: str, session: dict, websocket: WebSocket):
        """Send current UCF metrics."""
        # Load actual UCF state from state files
        try:
            ucf_state = load_ucf_state()
        except Exception as e:
            logger.warning("Failed to load UCF state, using defaults: %s", e)
            from apps.backend.coordination.ucf_state_loader import get_default_state

            ucf_state = get_default_state()

        await self.send_personal_message(
            {
                "type": "ucf_state",
                "data": ucf_state,
                "timestamp": datetime.now(UTC).isoformat(),
            },
            websocket,
        )

    async def _load_history(self, session_key: str, agent_id: str, limit: int = 20) -> list[dict[str, str]]:
        """Load conversation history from Redis for cross-reload persistence."""
        try:
            from apps.backend.core.redis_client import get_redis

            redis = await get_redis()
            if not redis:
                return []
            key = f"helix:webchat:history:{session_key}:{agent_id}"
            raw = await redis.get(key)
            if not raw:
                return []
            history: list[dict[str, str]] = json.loads(raw)
            return history[-limit:] if len(history) > limit else history
        except Exception as exc:
            logger.debug("Could not load chat history from Redis: %s", exc)
            return []

    async def _save_history(self, session_key: str, agent_id: str, user_message: str, assistant_response: str) -> None:
        """Append an exchange to Redis history (7-day TTL, capped at 80 messages)."""
        try:
            from apps.backend.core.redis_client import get_redis

            redis = await get_redis()
            if not redis:
                return
            key = f"helix:webchat:history:{session_key}:{agent_id}"
            raw = await redis.get(key)
            history: list[dict[str, str]] = json.loads(raw) if raw else []
            history.append({"role": "user", "content": user_message})
            history.append({"role": "assistant", "content": assistant_response})
            if len(history) > 80:
                history = history[-80:]
            await redis.setex(key, 604800, json.dumps(history))  # 7-day TTL
        except Exception as exc:
            logger.debug("Could not save chat history to Redis: %s", exc)

    async def _fetch_agent_memories(self, user_id: str, agent_id: str, limit: int = 5) -> str:
        """Return a formatted string of recent memories for this user + agent."""
        try:
            async with get_db_session() as db:
                result = await db.execute(
                    select(AgentMemoryEntry.content, AgentMemoryEntry.summary)
                    .where(
                        AgentMemoryEntry.user_id == user_id,
                        AgentMemoryEntry.agent_name == agent_id,
                        AgentMemoryEntry.memory_type.in_(["conversation", "fact", "user_preference"]),
                    )
                    .order_by(AgentMemoryEntry.created_at.desc())
                    .limit(limit)
                )
                rows = result.fetchall()
                if not rows:
                    return ""
                parts = [summary or content[:200] for content, summary in rows]
                return "\n".join(f"- {p}" for p in parts)
        except Exception as _mem_exc:
            logger.debug("Could not fetch agent memories for %s/%s: %s", user_id, agent_id, _mem_exc)
            return ""

    async def _persist_exchange(
        self,
        session_id: str,
        user_id: str,
        agent_id: str,
        user_message: str,
        agent_response: str,
    ) -> None:
        """Persist a user↔agent exchange to saas_conversations/saas_messages and create a memory entry."""
        try:
            async with get_db_session() as db:
                cache_key = f"{user_id}:{session_id}"
                conv_id = self._conversation_id_cache.get(cache_key)

                if not conv_id:
                    conv_id = str(uuid.uuid4())
                    db.add(
                        Conversation(
                            id=conv_id,
                            user_id=user_id,
                            title=f"Chat with {agent_id.capitalize()}",
                            metadata_json={
                                "session_id": session_id,
                                "platform": "web_chat",
                                "agents": [agent_id],
                            },
                        )
                    )
                    self._conversation_id_cache[cache_key] = conv_id

                now = datetime.now(UTC).replace(tzinfo=None)
                db.add(
                    Message(
                        id=str(uuid.uuid4()),
                        conversation_id=conv_id,
                        role="user",
                        content=user_message,
                        agent_id=agent_id,
                        created_at=now,
                    )
                )
                db.add(
                    Message(
                        id=str(uuid.uuid4()),
                        conversation_id=conv_id,
                        role="assistant",
                        content=agent_response,
                        agent_id=agent_id,
                        created_at=now,
                    )
                )
                # Create a memory entry so this exchange is available for future injection
                db.add(
                    AgentMemoryEntry(
                        id=str(uuid.uuid4()),
                        agent_name=agent_id,
                        memory_type="conversation",
                        content=f"User: {user_message}\nAgent: {agent_response[:300]}",
                        summary=agent_response[:150],
                        platform="web",
                        user_id=user_id,
                        source_id=conv_id,
                    )
                )
        except Exception as _persist_exc:
            logger.warning("Failed to persist chat exchange (session=%s): %s", session_id, _persist_exc)

    async def stream_ucf_metrics(self):
        """Continuously stream UCF metrics to all connected clients."""
        while True:
            try:
                state = load_ucf_state()
                ucf_update = {
                    "type": "ucf_update",
                    "harmony": state.get("harmony", 0),
                    "resilience": state.get("resilience", 0),
                    "throughput": state.get("throughput", 0),
                    "friction": state.get("friction", 0),
                    "focus": state.get("focus", 0),
                    "velocity": state.get("velocity", 0),
                    "performance_score": state.get("performance_score", 0),
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            except Exception as e:
                logger.warning("Failed to load UCF state: %s", e)
                ucf_update = {
                    "type": "ucf_update",
                    "harmony": 0,
                    "resilience": 0,
                    "throughput": 0,
                    "friction": 0,
                    "focus": 0,
                    "velocity": 0,
                    "performance_score": 0,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "_default": True,
                    "degraded_reason": "ucf_engine_unavailable",
                }

            try:
                await self.broadcast(ucf_update)
                await asyncio.sleep(5)  # Update every 5 seconds
            except Exception as e:
                logger.error("Error streaming UCF metrics: %s", e)
                await asyncio.sleep(10)


# Global connection manager instance
connection_manager = WebChatConnectionManager()

"""GitHub Integration - auto commit, push, and publish to GitHub Pages."""
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)


class GitHubIntegration:
    """Handles automatic git commits, pushes, and GitHub Pages publishing."""

    def __init__(self, repo_path: str = ".", remote: str = "origin", branch: str = "main"):
        self.repo_path = Path(repo_path).resolve()
        self.remote = remote
        self.branch = branch
        self.commit_history: list = []
        self._running = False

    async def initialize(self):
        self._running = True
        event_bus.subscribe(EventType.DASHBOARD_HTML, self._on_dashboard_ready)
        event_bus.subscribe(EventType.REPORT_GENERATED, self._on_report_ready)
        event_bus.subscribe(EventType.GITHUB_COMMIT, self._on_commit_request)
        logger.info("GitHub Integration initialized")

    async def _on_dashboard_ready(self, event: Event):
        await self.auto_commit("Update dashboard")

    async def _on_report_ready(self, event: Event):
        report_type = event.payload.get("report_type", "report")
        await self.auto_commit(f"Update {report_type} report")

    async def _on_commit_request(self, event: Event):
        message = event.payload.get("message", "Auto-update")
        await self.auto_commit(message)

    async def auto_commit(self, message: str) -> bool:
        try:
            self._run_git("add", "-A")
            status = self._run_git("status", "--porcelain")
            if not status.strip():
                logger.info("No changes to commit")
                return False

            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            full_message = f"{message}\n\nAuto-generated at {timestamp}\nBy: CommodityOS Meta-Agent"

            self._run_git("commit", "-m", full_message)
            logger.info(f"Committed: {message}")

            self.commit_history.append({
                "timestamp": time.time(),
                "message": message,
                "status": "committed",
            })

            await event_bus.emit(EventType.GITHUB_COMMIT, {
                "message": message,
                "status": "success",
            }, source="github_integration")

            return True
        except Exception as e:
            logger.error(f"Commit failed: {e}")
            self.commit_history.append({
                "timestamp": time.time(),
                "message": message,
                "status": "failed",
                "error": str(e),
            })
            return False

    async def push(self) -> bool:
        try:
            self._run_git("push", self.remote, self.branch)
            logger.info(f"Pushed to {self.remote}/{self.branch}")

            await event_bus.emit(EventType.GITHUB_PUSH, {
                "remote": self.remote,
                "branch": self.branch,
                "status": "success",
            }, source="github_integration")

            return True
        except Exception as e:
            logger.error(f"Push failed: {e}")
            return False

    async def publish_to_pages(self) -> bool:
        try:
            self._run_git("add", "output/")
            status = self._run_git("status", "--porcelain")
            if status.strip():
                self._run_git("commit", "-m", "Update GitHub Pages content")
                self._run_git("push", self.remote, self.branch)

            await event_bus.emit(EventType.GITHUB_PAGES, {
                "status": "published",
            }, source="github_integration")

            logger.info("Published to GitHub Pages")
            return True
        except Exception as e:
            logger.error(f"GitHub Pages publish failed: {e}")
            return False

    async def full_publish_cycle(self):
        committed = await self.auto_commit("Auto-update: commodity data")
        if committed:
            pushed = await self.push()
            if pushed:
                await self.publish_to_pages()

    def _run_git(self, *args) -> str:
        cmd = ["git"] + list(args)
        result = subprocess.run(
            cmd,
            cwd=str(self.repo_path),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0 and args[0] != "status":
            raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr}")
        return result.stdout

    def get_status(self) -> Dict[str, Any]:
        try:
            log = self._run_git("log", "--oneline", "-10")
            branches = self._run_git("branch", "-a")
            return {
                "recent_commits": log.strip().split("\n") if log.strip() else [],
                "branches": branches.strip(),
                "commit_history_count": len(self.commit_history),
            }
        except Exception:
            return {"error": "Git not available"}

    async def shutdown(self):
        self._running = False
        logger.info("GitHub Integration shut down")

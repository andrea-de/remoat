import { EventEmitter } from 'events';
import * as http from 'http';
import { CDP_PORTS } from '../utils/cdpPorts';

import { logger } from '../utils/logger';
import { extractProjectNameFromPath } from '../utils/pathUtils';
import { CdpService, CdpServiceOptions } from './cdpService';
import { ApprovalDetector } from './approvalDetector';
import { ErrorPopupDetector } from './errorPopupDetector';
import { PlanningDetector } from './planningDetector';
import { UserMessageDetector } from './userMessageDetector';

/**
 * Pool that manages independent CdpService instances per workspace.
 *
 * Each workspace owns its own WebSocket / contexts / pendingCalls, so
 * switching to workspace B while workspace A's ResponseMonitor is polling
 * does not destroy A's WebSocket.
 *
 * Emits workspace lifecycle events:
 * - `workspace:disconnected` (projectName: string)
 * - `workspace:reconnected` (projectName: string)
 * - `workspace:reconnectFailed` (projectName: string)
 */
export class CdpConnectionPool extends EventEmitter {
    private readonly connections = new Map<string, CdpService>();
    private readonly approvalDetectors = new Map<string, ApprovalDetector>();
    private readonly errorPopupDetectors = new Map<string, ErrorPopupDetector>();
    private readonly planningDetectors = new Map<string, PlanningDetector>();
    private readonly userMessageDetectors = new Map<string, UserMessageDetector>();
    private readonly connectingPromises = new Map<string, Promise<CdpService>>();
    private readonly lastUsed = new Map<string, number>();
    private readonly cdpOptions: CdpServiceOptions;
    private maxConnections: number = 3;

    constructor(maxConnections: number = 3, cdpOptions: CdpServiceOptions = {}) {
        super();
        this.maxConnections = maxConnections;
        this.cdpOptions = cdpOptions;
    }

    /**
     * Update the max number of allowed connections.
     */
    setMaxConnections(max: number): void {
        this.maxConnections = max;
    }

    /**
     * Get the current max number of allowed connections.
     */
    getMaxConnections(): number {
        return this.maxConnections;
    }

    /**
     * Aggressively close any Antigravity window/target that does not belong to the active project.
     * This helps prevent resource exhaustion on limited VPS hardware.
     */
    async cleanupExtraTargets(activeProjectName?: string): Promise<void> {
        logger.debug(`[CdpConnectionPool] Starting aggressive target cleanup (activeProject="${activeProjectName || 'none'}")...`);
        const ports = this.cdpOptions.portsToScan || [...CDP_PORTS];
        
        for (const port of ports) {
            try {
                const list = await this.getJson(`http://127.0.0.1:${port}/json/list`);
                const targetsToClose = list.filter((t: any) => {
                    if (t.type !== 'page' || !t.webSocketDebuggerUrl) return false;
                    
                    // Always ignore Launchpad and internal agents
                    if (t.title?.includes('Launchpad') || t.url?.includes('workbench-jetski-agent')) return false;
                    
                    // If it's a workbench (or looks like a window loading)
                    if (t.url?.includes('workbench') || t.title === '' || t.title === 'Antigravity' || t.title === 'Cascade') {
                        if (!activeProjectName) return true; // Close all if no active project
                        
                        // If it has a title, check if it matches the active project
                        if (t.title && t.title !== 'Antigravity' && t.title !== 'Cascade') {
                            return !t.title.toLowerCase().includes(activeProjectName.toLowerCase());
                        }
                        
                        // If it has NO title yet (loading), and we already have an active connection elsewhere,
                        // it's likely a zombie or a duplicate window.
                        return true;
                    }
                    
                    return false;
                });

                for (const target of targetsToClose) {
                    logger.info(`[CdpConnectionPool] Auto-closing unauthorized target on port ${port}: "${target.title || '(no title)'}" (id=${target.id})`);
                    await this.closeTargetById(port, target.id).catch(() => {});
                }
            } catch {
                // Port not responding, skip
            }
        }
    }

    private async getJson(url: string): Promise<any[]> {
        return new Promise((resolve, reject) => {
            const req = http.get(url, (res) => {
                if (res.statusCode && (res.statusCode < 200 || res.statusCode >= 300)) {
                    res.resume();
                    reject(new Error(`HTTP ${res.statusCode}`));
                    return;
                }
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { reject(e); } });
            });
            req.on('error', reject);
            req.setTimeout(3000, () => { req.destroy(); reject(new Error('Timeout')); });
        });
    }

    private async closeTargetById(port: number, targetId: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const req = http.request({
                hostname: '127.0.0.1',
                port: port,
                path: `/json/close/${targetId}`,
                method: 'PUT'
            }, (res) => {
                res.resume();
                if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) resolve();
                else reject(new Error(`Status ${res.statusCode}`));
            });
            req.on('error', reject);
            req.end();
        });
    }

    /**
     * Get a CdpService for the given workspace path.
     * Creates a new connection and caches it if not already connected.
     * Prevents concurrent connections via Promise locking.
     *
     * @param workspacePath Full path of the workspace
     * @returns Connected CdpService
     */
    async getOrConnect(workspacePath: string): Promise<CdpService> {
        const projectName = this.extractProjectName(workspacePath);

        // Return existing connection if available
        const existing = this.connections.get(projectName);
        if (existing) {
            if (existing.isConnected()) {
                try {
                    // Re-validate that the still-open window is actually bound to this workspace.
                    await existing.discoverAndConnectForWorkspace(workspacePath);
                    this.lastUsed.set(projectName, Date.now());
                    return existing;
                } catch {
                    // Connection dropped during re-validation; close WebSocket and clean up
                    existing.disconnect().catch(() => {});
                    this.connections.delete(projectName);
                }
            } else {
                // Stale disconnected entry (e.g. reconnect was disabled) — clean up
                this.connections.delete(projectName);
            }
        }

        // Wait for the pending connection promise if one exists (prevents concurrent connections)
        const pending = this.connectingPromises.get(projectName);
        if (pending) {
            return pending;
        }

        // PRE-CONNECTION CLEANUP:
        // Before starting a new heavy Antigravity instance, ensure we are within limits
        // and clean up any "ghost" windows that shouldn't be there.
        try {
            await this.cleanupExtraTargets().catch(() => {});
            await this.enforceLimit(projectName);
        } catch (err) {
            logger.warn('[CdpConnectionPool] Pre-connection cleanup failed:', err);
        }

        // Start a new connection
        const connectPromise = this.createAndConnect(workspacePath, projectName);
        this.connectingPromises.set(projectName, connectPromise);

        try {
            const cdp = await connectPromise;
            this.lastUsed.set(projectName, Date.now());
            return cdp;
        } finally {
            this.connectingPromises.delete(projectName);
        }
    }

    /**
     * If the connection limit is reached, close the oldest (LRU) project.
     * @param excludeProject Project to keep (usually the one just opened)
     */
    private enforceLimit(excludeProject: string): void {
        const active = this.getActiveWorkspaceNames();
        if (active.length <= this.maxConnections) return;

        logger.info(`[CdpConnectionPool] Limit reached (${this.maxConnections}). Closing oldest project...`);

        // Find oldest based on lastUsed
        let oldestProject: string | null = null;
        let oldestTime = Infinity;

        for (const name of active) {
            if (name === excludeProject) continue;
            const time = this.lastUsed.get(name) || 0;
            if (time < oldestTime) {
                oldestTime = time;
                oldestProject = name;
            }
        }

        if (oldestProject) {
            logger.info(`[CdpConnectionPool] Auto-closing LRU project: ${oldestProject}`);
            this.closeBrowserWorkspace(oldestProject).catch((err) => {
                logger.error(`[CdpConnectionPool] Failed to auto-close ${oldestProject}:`, err);
            });
        }
    }

    /**
     * Get a connected CdpService (read-only).
     * Returns null if not connected.
     */
    getConnected(projectName: string): CdpService | null {
        const cdp = this.connections.get(projectName);
        if (cdp && cdp.isConnected()) {
            return cdp;
        }
        return null;
    }

    /**
     * Disconnect the specified workspace.
     */
    disconnectWorkspace(projectName: string): void {
        const cdp = this.connections.get(projectName);
        if (cdp) {
            cdp.disconnect().catch((err) => {
                logger.error(`[CdpConnectionPool] Error while disconnecting ${projectName}:`, err);
            });
            this.connections.delete(projectName);
        }

        const detector = this.approvalDetectors.get(projectName);
        if (detector) {
            detector.stop();
            this.approvalDetectors.delete(projectName);
        }

        const errorPopupDetector = this.errorPopupDetectors.get(projectName);
        if (errorPopupDetector) {
            errorPopupDetector.stop();
            this.errorPopupDetectors.delete(projectName);
        }

        const planningDetector = this.planningDetectors.get(projectName);
        if (planningDetector) {
            planningDetector.stop();
            this.planningDetectors.delete(projectName);
        }

        const userMsgDetector = this.userMessageDetectors.get(projectName);
        if (userMsgDetector) {
            userMsgDetector.stop();
            this.userMessageDetectors.delete(projectName);
        }

        this.lastUsed.delete(projectName);
    }

    /**
     * Completely close the Antigravity instance for the specified workspace via CDP.
     */
    async closeBrowserWorkspace(projectName: string): Promise<void> {
        const cdp = this.connections.get(projectName);
        if (cdp) {
            try {
                await cdp.closeBrowserTarget();
            } catch (err) {
                logger.error(`[CdpConnectionPool] Error while closing browser for ${projectName}:`, err);
            }
        }
        this.disconnectWorkspace(projectName);
        // Clean up any remaining zombie targets for this project
        await this.cleanupExtraTargets().catch(() => {});
    }

    /**
     * Disconnect all workspace connections.
     */
    disconnectAll(): void {
        for (const projectName of [...this.connections.keys()]) {
            this.disconnectWorkspace(projectName);
        }
    }

    /**
     * Register an approval detector for a workspace.
     */
    registerApprovalDetector(projectName: string, detector: ApprovalDetector): void {
        // Stop existing detector
        const existing = this.approvalDetectors.get(projectName);
        if (existing && existing.isActive()) {
            existing.stop();
        }
        this.approvalDetectors.set(projectName, detector);
    }

    /**
     * Get the approval detector for a workspace.
     */
    getApprovalDetector(projectName: string): ApprovalDetector | undefined {
        return this.approvalDetectors.get(projectName);
    }

    /**
     * Register an error popup detector for a workspace.
     */
    registerErrorPopupDetector(projectName: string, detector: ErrorPopupDetector): void {
        // Stop existing detector
        const existing = this.errorPopupDetectors.get(projectName);
        if (existing && existing.isActive()) {
            existing.stop();
        }
        this.errorPopupDetectors.set(projectName, detector);
    }

    /**
     * Get the error popup detector for a workspace.
     */
    getErrorPopupDetector(projectName: string): ErrorPopupDetector | undefined {
        return this.errorPopupDetectors.get(projectName);
    }

    /**
     * Register a planning detector for a workspace.
     */
    registerPlanningDetector(projectName: string, detector: PlanningDetector): void {
        // Stop existing detector
        const existing = this.planningDetectors.get(projectName);
        if (existing && existing.isActive()) {
            existing.stop();
        }
        this.planningDetectors.set(projectName, detector);
    }

    /**
     * Get the planning detector for a workspace.
     */
    getPlanningDetector(projectName: string): PlanningDetector | undefined {
        return this.planningDetectors.get(projectName);
    }

    /**
     * Register a user message detector for a workspace.
     */
    registerUserMessageDetector(projectName: string, detector: UserMessageDetector): void {
        const existing = this.userMessageDetectors.get(projectName);
        if (existing && existing.isActive()) {
            existing.stop();
        }
        this.userMessageDetectors.set(projectName, detector);
    }

    /**
     * Get the user message detector for a workspace.
     */
    getUserMessageDetector(projectName: string): UserMessageDetector | undefined {
        return this.userMessageDetectors.get(projectName);
    }

    /**
     * Return a list of workspace names with active connections.
     */
    getActiveWorkspaceNames(): string[] {
        const active: string[] = [];
        for (const [name, cdp] of this.connections) {
            if (cdp.isConnected()) {
                active.push(name);
            }
        }
        return active;
    }

    /**
     * Extract the project name from a workspace path.
     */
    extractProjectName(workspacePath: string): string {
        return extractProjectNameFromPath(workspacePath) || workspacePath;
    }

    /**
     * Create a new CdpService and connect to the workspace.
     */
    private async createAndConnect(workspacePath: string, projectName: string): Promise<CdpService> {
        // Disconnect old connection if exists
        const old = this.connections.get(projectName);
        if (old) {
            await old.disconnect().catch(() => {});
            this.connections.delete(projectName);
        }

        const cdp = new CdpService(this.cdpOptions);

        // Auto-cleanup on disconnect
        cdp.on('disconnected', () => {
            logger.error(`[CdpConnectionPool] Workspace "${projectName}" disconnected`);
            this.emit('workspace:disconnected', projectName);
            // Only remove from Map when reconnection fails
            // (CdpService attempts reconnection internally, so we don't remove here)
        });

        cdp.on('reconnected', () => {
            logger.info(`[CdpConnectionPool] Workspace "${projectName}" reconnected`);
            this.emit('workspace:reconnected', projectName);
        });

        cdp.on('reconnectFailed', () => {
            logger.error(`[CdpConnectionPool] Reconnection failed for workspace "${projectName}". Removing from pool`);
            this.emit('workspace:reconnectFailed', projectName);
            this.disconnectWorkspace(projectName);
        });

        // Connect to the workspace
        await cdp.discoverAndConnectForWorkspace(workspacePath);
        this.connections.set(projectName, cdp);

        return cdp;
    }
}

// Repair v2 — health checker dashboard.
//
// Settings live in the global Settings page; this controller only handles
// status, run/stop, and history. Polls /api/repair/status while a run is
// active so the UI reflects live progress.
class RepairManager {
    constructor() {
        this.api = (window.API || '/api').replace(/\/$/, '');
        this.statusTimer = null;
        this.activeRunId = null;
        this.brokenState = {items: [], page: 1, pageSize: 25};
        this.repairConfig = {};
        this.latestStatus = {};
        // "Act on broken" modal state. actBrokenBlockedReason is null when the
        // action is available and otherwise holds the human-readable reason it
        // is not — the button stays clickable and reports that reason.
        this.actBrokenStep = 'picker';
        this.actBrokenBlockedReason = null;
        this.bind();
        this.loadAll();
    }

    bind() {
        const $ = (id) => document.getElementById(id);
        $('runNowBtn')?.addEventListener('click', () => this.openRunModal());
        $('stopRunBtn')?.addEventListener('click', () => this.stopRun());
        // "Act on broken": select-MANY over the whole broken set. REPAIR /
        // PRUNE / RE-GRAB compose, so the modal collects any combination and
        // sends them in a single request.
        $('actBrokenBtn')?.addEventListener('click', () => this.openActBrokenModal());
        $('actBrokenForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.actBrokenAdvance();
        });
        $('actBrokenBackBtn')?.addEventListener('click', () => {
            if (this.actBrokenStep === 'confirm') {
                this.setActBrokenStep('picker');
                return;
            }
            $('actBrokenModal')?.close?.();
        });
        $('clearStateBtn')?.addEventListener('click', () => this.openClearStateModal());
        $('viewBrokenBtn')?.addEventListener('click', () => this.openBrokenModal());
        $('refreshHistoryBtn')?.addEventListener('click', () => this.loadHistory());
        $('refreshBrokenBtn')?.addEventListener('click', () => this.loadBroken());
        $('clearHistoryBtn')?.addEventListener('click', () => this.clearHistory());
        $('runRepairForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.runNow();
        });
        $('clearStateForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.clearState();
        });
        $('recheckMediaForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.recheckMedia();
        });
    }

    async loadAll() {
        await Promise.all([this.loadRepairConfig(), this.loadStatus(), this.loadHistory(), this.loadArrs()]);
    }

    async loadRepairConfig() {
        try {
            this.repairConfig = await this.fetchJSON(`${this.api}/repair/config`) || {};
        } catch (e) {
            console.error('Failed to load repair config', e);
            this.repairConfig = {};
        }
    }

    openRunModal() {
        const modal = document.getElementById('runRepairModal');
        if (!modal) return;
        const ignore = document.getElementById('runIgnoreLastChecked');
        const unrestrictLink = document.getElementById('runUnrestrictLink');
        if (ignore) ignore.checked = false;
        if (unrestrictLink) unrestrictLink.checked = false;
        const runRepair = document.getElementById('runRepair');
        const runPrune = document.getElementById('runPrune');
        const runRegrab = document.getElementById('runRegrab');
        if (runRepair) runRepair.checked = this.repairConfig.repair !== false;
        if (runPrune) runPrune.checked = !!this.repairConfig.prune;
        if (runRegrab) runRegrab.checked = !!this.repairConfig.regrab;
        const defaultProtocol = this.repairConfig.skip_nzb_repair ? 'torrent' : 'all';
        const protocol = document.querySelector(`input[name="runProtocol"][value="${defaultProtocol}"]`)
            || document.getElementById('runProtocolAll');
        if (protocol) protocol.checked = true;
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    openClearStateModal() {
        const modal = document.getElementById('clearStateModal');
        if (!modal) return;
        document.getElementById('clearStateError')?.classList.add('hidden');
        document.querySelectorAll('input[name="repair_state"]').forEach((input) => {
            input.checked = false;
        });
        this.updateClearStateCounts(this.latestStatus.health_counts || {});
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    openBrokenModal() {
        const modal = document.getElementById('brokenModal');
        if (!modal) return;
        // Fetch fresh data on every open.
        this.loadBroken();
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    isBrokenModalOpen() {
        const modal = document.getElementById('brokenModal');
        return !!(modal && modal.open);
    }

    updateBrokenCount(n) {
        const badge = document.getElementById('brokenCountBadge');
        if (badge) {
            badge.textContent = n;
            badge.classList.toggle('hidden', n === 0);
        }
        const modalCount = document.getElementById('brokenModalCount');
        if (modalCount) modalCount.textContent = n;
    }

    async loadArrs() {
        try {
            const arrs = await this.fetchJSON(`${this.api}/arrs`);
            const sel = document.getElementById('recheckArr');
            if (!sel) return;
            const placeholder = sel.querySelector('option[value=""]');
            sel.innerHTML = '';
            if (placeholder) sel.appendChild(placeholder);
            for (const a of arrs || []) {
                if (!a || !a.name) continue;
                const opt = document.createElement('option');
                opt.value = a.name;
                opt.textContent = a.name;
                sel.appendChild(opt);
            }
        } catch (e) {
            console.error('Failed to load arrs', e);
        }
    }

    // fixComponentMeta maps a component key to its display label + one-liner,
    // shared by the confirm dialogs and toasts so all surfaces read identically.
    fixComponentMeta(component) {
        return ({
            repair: {label: 'REPAIR', desc: 're-acquire on the same or a backup provider'},
            prune: {label: 'PRUNE', desc: 'delete decypharr-side only — the arr keeps monitoring and re-searches'},
            regrab: {label: 'RE-GRAB', desc: 'arr delete + blocklist + search'},
        })[component] || {label: String(component || '').toUpperCase(), desc: ''};
    }

    async recheckMedia() {
        const $ = (id) => document.getElementById(id);
        const mediaId = $('recheckMediaId').value.trim();
        if (!mediaId) {
            this.toast('Media id is required', 'warning');
            return;
        }
        // CHECK always runs; the selected components act on whatever probes broken.
        const body = {
            arr: $('recheckArr').value,
            media_id: mediaId,
            actions: {
                repair: !!$('recheckRepair')?.checked,
                prune: !!$('recheckPrune')?.checked,
                regrab: !!$('recheckRegrab')?.checked,
            },
        };
        const btn = $('recheckMediaBtn');
        const out = $('recheckMediaResult');
        btn.disabled = true;
        out.classList.add('hidden');
        out.textContent = '';
        try {
            const res = await fetch(`${this.api}/repair/recheck/media`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body),
            });
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) {
                const msg = (data && (data.error || data.message)) || text || `HTTP ${res.status}`;
                throw new Error(msg);
            }
            // Server kicks the recheck off in the background and returns a
            // run record immediately. Reload so the dashboard reflects the
            // new active run; status polling takes it from there.
            this.toast('Recheck started', 'success');
            window.location.reload();
        } catch (e) {
            out.classList.remove('hidden');
            out.innerHTML = `<span class="text-error">Recheck failed: ${this.escape(e.message)}</span>`;
            btn.disabled = false;
        }
    }

    renderRecheckResult(container, run) {
        if (!container) return;
        if (!run) {
            container.classList.add('hidden');
            return;
        }
        container.classList.remove('hidden');
        const stats = run.stats || {};
        const status = run.status || 'unknown';
        const cls = {
            running: 'badge-info',
            completed: 'badge-success',
            failed: 'badge-error',
            cancelled: 'badge-warning',
        }[status] || 'badge-ghost';
        container.innerHTML = `
            <div class="flex flex-wrap gap-3 items-center">
                <span class="badge ${cls}">${this.escape(status)}</span>
                <span class="font-mono text-xs">${this.escape(run.id || '')}</span>
                ${run.source ? `<span class="opacity-70 text-xs">${this.escape(run.source)}</span>` : ''}
            </div>
            <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-2 mt-3 text-xs">
                <div>Checked: <strong>${stats.probed ?? 0}</strong></div>
                <div class="${stats.broken ? 'text-error' : ''}">Broken: <strong>${stats.broken ?? 0}</strong></div>
                <div class="${stats.reacquired ? 'text-success' : ''}">Repaired: <strong>${stats.reacquired ?? 0}</strong></div>
                <div class="${stats.repair_failed ? 'text-error' : ''}" title="${RepairManager.STAT_TITLES.repair_failed}">Repair fail: <strong>${stats.repair_failed ?? 0}</strong></div>
                <div class="${stats.repair_skipped_unsupported ? 'text-warning' : ''}" title="${RepairManager.STAT_TITLES.repair_skipped_unsupported}">Repair n/a: <strong>${stats.repair_skipped_unsupported ?? 0}</strong></div>
                <div class="${stats.pruned ? 'text-warning' : ''}">Pruned: <strong>${stats.pruned ?? 0}</strong></div>
                <div class="${stats.prune_skipped_not_eligible ? 'text-warning' : ''}" title="${RepairManager.STAT_TITLES.prune_skipped_not_eligible}">Prune skipped: <strong>${stats.prune_skipped_not_eligible ?? 0}</strong></div>
                <div class="${stats.regrabbed ? 'text-info' : ''}">Re-grabbed: <strong>${stats.regrabbed ?? 0}</strong></div>
                <div class="${stats.regrab_failed ? 'text-error' : ''}" title="${RepairManager.STAT_TITLES.regrab_failed}">Re-grab fail: <strong>${stats.regrab_failed ?? 0}</strong></div>
                <div class="${stats.regrab_skipped_no_arr_link ? 'text-warning' : ''}" title="${RepairManager.STAT_TITLES.regrab_skipped_no_arr_link}">Re-grab no arr: <strong>${stats.regrab_skipped_no_arr_link ?? 0}</strong></div>
            </div>
            ${run.error ? `<div class="mt-2 text-error text-xs">${this.escape(run.error)}</div>` : ''}
        `;
    }

    escape(s) {
        const div = document.createElement('div');
        div.textContent = s == null ? '' : String(s);
        return div.innerHTML;
    }

    async runNow() {
        const btn = document.getElementById('runRepairSubmitBtn');
        if (btn) btn.disabled = true;
        try {
            const ignoreLastChecked = !!document.getElementById('runIgnoreLastChecked')?.checked;
            const unrestrictLink = !!document.getElementById('runUnrestrictLink')?.checked;
            const protocol = document.querySelector('input[name="runProtocol"]:checked')?.value || 'all';
            const repair = !!document.getElementById('runRepair')?.checked;
            const prune = !!document.getElementById('runPrune')?.checked;
            const regrab = !!document.getElementById('runRegrab')?.checked;
            const res = await fetch(`${this.api}/repair/run`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    ignore_last_checked: ignoreLastChecked,
                    unrestrict_link: unrestrictLink,
                    protocol,
                    repair,
                    prune,
                    regrab,
                }),
            });
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(txt || `HTTP ${res.status}`);
            }
            document.getElementById('runRepairModal')?.close?.();
            this.toast(ignoreLastChecked ? 'Sweep started, including freshly checked entries' : 'Sweep started', 'success');
            await this.loadStatus();
        } catch (e) {
            this.toast(`Run failed: ${e.message}`, 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    // ---- "Act on broken" (select-many) ----------------------------------

    // selectedFixComponents returns the ticked components in canonical
    // REPAIR → PRUNE → RE-GRAB order.
    selectedFixComponents() {
        return ['repair', 'prune', 'regrab'].filter((c) =>
            !!document.querySelector(`#actBrokenModal [data-act-component="${c}"]`)?.checked);
    }

    openActBrokenModal() {
        // The button is never inert: when the action is unavailable, say why
        // instead of silently doing nothing.
        if (this.actBrokenBlockedReason) {
            this.toast(this.actBrokenBlockedReason, 'info');
            return;
        }
        const modal = document.getElementById('actBrokenModal');
        if (!modal) return;
        document.querySelectorAll('#actBrokenModal [data-act-component]').forEach((cb) => {
            cb.checked = false;
        });
        const count = document.getElementById('actBrokenTargetCount');
        if (count) count.textContent = (this.latestStatus.health_counts || {}).broken || 0;
        this.setActBrokenStep('picker');
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    setActBrokenStep(step) {
        this.actBrokenStep = step === 'confirm' ? 'confirm' : 'picker';
        const onConfirm = this.actBrokenStep === 'confirm';
        document.getElementById('actBrokenPicker')?.classList.toggle('hidden', onConfirm);
        document.getElementById('actBrokenConfirm')?.classList.toggle('hidden', !onConfirm);
        const back = document.getElementById('actBrokenBackBtn');
        if (back) back.textContent = onConfirm ? 'Back' : 'Cancel';
        const apply = document.getElementById('actBrokenApplyBtn');
        if (apply) {
            apply.disabled = false;
            apply.textContent = onConfirm
                ? `Run ${this.selectedFixComponents().map((c) => this.fixComponentMeta(c).label).join(' + ')}`
                : 'Continue';
        }
        if (!onConfirm) document.getElementById('actBrokenError')?.classList.add('hidden');
    }

    // actBrokenAdvance drives picker → confirm → send. Nothing is posted until
    // the operator has seen the confirmation naming the exact components.
    actBrokenAdvance() {
        const components = this.selectedFixComponents();
        const error = document.getElementById('actBrokenError');
        if (!components.length) {
            if (error) {
                error.textContent = 'Pick at least one component to run.';
                error.classList.remove('hidden');
            }
            return;
        }
        error?.classList.add('hidden');
        if (this.actBrokenStep !== 'confirm') {
            this.renderActBrokenConfirm(components);
            this.setActBrokenStep('confirm');
            return;
        }
        this.fixBroken(components);
    }

    // renderActBrokenConfirm names EXACTLY which components will run and calls
    // out the destructive ones (PRUNE / RE-GRAB) before anything is sent.
    renderActBrokenConfirm(components) {
        const labels = components.map((c) => this.fixComponentMeta(c).label);
        const target = document.getElementById('actBrokenConfirmComponents');
        if (target) target.textContent = labels.join(' + ');
        const detail = document.getElementById('actBrokenConfirmDetail');
        if (!detail) return;
        const broken = (this.latestStatus.health_counts || {}).broken || 0;
        const destructive = components.filter((c) => c !== 'repair');
        const rows = components.map((c) => {
            const meta = this.fixComponentMeta(c);
            return `<li><span class="font-semibold">${this.escape(meta.label)}</span> — ${this.escape(meta.desc)}</li>`;
        }).join('');
        detail.innerHTML = `
            <p>Running on all <strong>${broken}</strong> currently broken entr${broken === 1 ? 'y' : 'ies'}:</p>
            <ul class="list-disc list-inside mt-1 space-y-0.5">${rows}</ul>
            ${destructive.length
                ? `<p class="mt-2 font-semibold">${destructive.map((c) => this.fixComponentMeta(c).label).join(' and ')} delete${destructive.length === 1 ? 's' : ''} files. This cannot be undone.</p>`
                : `<p class="mt-2 opacity-70">REPAIR is non-destructive — nothing is deleted.</p>`}
        `;
    }

    // fixBroken posts /repair/fix ONCE for every selected component over the
    // whole broken set. components ⊆ {repair, prune, regrab} and compose, so
    // two ticked components mean one request with both flags true.
    async fixBroken(components) {
        const list = (Array.isArray(components) ? components : [components]).filter(Boolean);
        if (!list.length) return;
        const labels = list.map((c) => this.fixComponentMeta(c).label).join(' + ');
        const apply = document.getElementById('actBrokenApplyBtn');
        if (apply) apply.disabled = true;
        try {
            const res = await fetch(`${this.api}/repair/fix`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    actions: {
                        repair: list.includes('repair'),
                        prune: list.includes('prune'),
                        regrab: list.includes('regrab'),
                    },
                }),
            });
            const text = await res.text();
            if (!res.ok) throw new Error(text || `HTTP ${res.status}`);
            document.getElementById('actBrokenModal')?.close?.();
            this.toast(`${labels} started`, 'success');
            window.location.reload();
        } catch (e) {
            this.toast(`${labels} failed: ${e.message}`, 'error');
            if (apply) apply.disabled = false;
        }
    }

    async clearState() {
        const selected = [...document.querySelectorAll('input[name="repair_state"]:checked')]
            .map((input) => input.value);
        const error = document.getElementById('clearStateError');
        if (!selected.length) {
            if (error) {
                error.textContent = 'Select at least one state.';
                error.classList.remove('hidden');
            }
            return;
        }

        const btn = document.getElementById('clearStateSubmitBtn');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(`${this.api}/repair/clear-state`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({statuses: selected}),
            });
            const text = await res.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch { /* leave null */
            }
            if (!res.ok) throw new Error((data && (data.error || data.message)) || text || `HTTP ${res.status}`);
            document.getElementById('clearStateModal')?.close?.();
            this.toast(`Cleared ${data?.cleared ?? 0} repair state entr${data?.cleared === 1 ? 'y' : 'ies'}`, 'success');
            await Promise.all([this.loadStatus(), this.loadBroken()]);
        } catch (e) {
            this.toast(`Clear failed: ${e.message}`, 'error');
            if (error) {
                error.textContent = e.message;
                error.classList.remove('hidden');
            }
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async stopRun() {
        try {
            const res = await fetch(`${this.api}/repair/stop`, {method: 'POST'});
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(txt || `HTTP ${res.status}`);
            }
            this.toast('Stop requested', 'info');
            await this.loadStatus();
        } catch (e) {
            this.toast(`Stop failed: ${e.message}`, 'error');
        }
    }

    async clearHistory() {
        if (!confirm('Clear all run history?')) return;
        try {
            const res = await fetch(`${this.api}/repair/runs`, {method: 'DELETE'});
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            await this.loadHistory();
        } catch (e) {
            this.toast(`Clear failed: ${e.message}`, 'error');
        }
    }

    async loadStatus() {
        try {
            const status = await this.fetchJSON(`${this.api}/repair/status`);
            this.renderStatus(status || {});
            this.scheduleStatusPoll(status);
        } catch (e) {
            console.error('Failed to load status', e);
        }
    }

    scheduleStatusPoll(status) {
        const isRunning = !!(status && status.active_run);
        const wasRunning = this.wasRunning === true;
        this.wasRunning = isRunning;
        // Run ended → refresh history. Only refetch the broken modal contents
        // when it's actually open; the count badge is already updated from
        // status.health_counts on every poll.
        if (wasRunning && !isRunning) {
            this.loadHistory();
            if (this.isBrokenModalOpen()) this.loadBroken();
        }
        if (this.statusTimer) {
            clearTimeout(this.statusTimer);
            this.statusTimer = null;
        }
        const delay = isRunning ? 2000 : 15000;
        this.statusTimer = setTimeout(() => this.loadStatus(), delay);
    }

    renderStatus(status) {
        this.latestStatus = status || {};
        const line = document.getElementById('repairStatusLine');
        const stop = document.getElementById('stopRunBtn');
        const run = document.getElementById('runNowBtn');
        const panel = document.getElementById('activeRunPanel');
        const grid = document.getElementById('healthCountsGrid');

        if (!status.enabled) {
            line.textContent = 'Repair is disabled. Enable it in Settings → Repair, or click "Run now" for a one-off check.';
        } else {
            const next = status.next_run_at ? new Date(status.next_run_at).toLocaleString() : 'unknown';
            line.textContent = `Repair enabled · next scheduled run: ${next}`;
        }

        const brokenCount = (status.health_counts || {}).broken || 0;
        this.updateBrokenCount(brokenCount);
        // "Act on broken" availability. NOTE: do NOT use daisyUI's .btn-disabled
        // here — it sets pointer-events:none, so the click never lands and the
        // control is indistinguishable from a broken one. Keep it clickable and
        // let the click explain itself via openActBrokenModal().
        this.actBrokenBlockedReason = status.active_run
            ? 'A repair run is already in progress — wait for it to finish before acting on broken entries.'
            : (brokenCount === 0 ? 'Nothing to act on: there are no broken entries right now.' : null);
        const act = document.getElementById('actBrokenBtn');
        if (act) {
            const blocked = !!this.actBrokenBlockedReason;
            act.setAttribute('aria-disabled', blocked ? 'true' : 'false');
            act.classList.toggle('opacity-50', blocked);
            act.classList.toggle('cursor-not-allowed', blocked);
            act.title = this.actBrokenBlockedReason || 'Act on all currently broken entries';
        }
        const clear = document.getElementById('clearStateBtn');
        if (clear) clear.disabled = !!status.active_run;
        const view = document.getElementById('viewBrokenBtn');
        if (view) view.disabled = brokenCount === 0;
        this.updateClearStateCounts(status.health_counts || {});

        if (status.active_run) {
            stop.disabled = false;
            run.disabled = true;
            panel.classList.remove('hidden');
            this.activeRunId = status.active_run.id;
            document.getElementById('activeRunStage').textContent = status.active_run.stage || 'running';
            document.getElementById('activeRunIdText').textContent = status.active_run.id || '-';
            document.getElementById('activeRunStarted').textContent = status.active_run.started_at
                ? new Date(status.active_run.started_at).toLocaleString()
                : '-';
            this.renderRunStats(document.getElementById('activeRunStats'), status.active_run.stats || {});
        } else {
            stop.disabled = true;
            run.disabled = false;
            panel.classList.add('hidden');
            this.activeRunId = null;
            document.getElementById('activeRunStage').textContent = '-';
            document.getElementById('activeRunIdText').textContent = '-';
            document.getElementById('activeRunStarted').textContent = '-';
        }

        const counts = status.health_counts || {};
        const order = ['healthy', 'broken', 'repairing', 'stale', 'unknown', 'unsupported'];
        grid.innerHTML = '';
        for (const key of order) {
            const n = counts[key] || 0;
            const card = document.createElement('div');
            card.className = 'stat bg-base-200 rounded-box p-3';
            card.innerHTML = `
                <div class="stat-title text-xs capitalize">${key}</div>
                <div class="stat-value text-lg ${this.healthColor(key)}">${n}</div>
            `;
            grid.appendChild(card);
        }
    }

    updateClearStateCounts(counts) {
        document.querySelectorAll('[data-clear-state-count]').forEach((el) => {
            const status = el.getAttribute('data-clear-state-count');
            el.textContent = counts?.[status] || 0;
        });
    }

    renderRunStats(container, stats) {
        if (!container) return;
        // Grouped by component: the outcome first, then that component's
        // failures and its principled declines. A decline is not a failure, so
        // it gets its own tile rather than being folded into the failure count.
        const fields = [
            ['candidates', 'Candidates'],
            ['probed', 'Checked'],
            ['broken', 'Broken'],
            ['reacquired', 'Repaired'],
            ['repair_failed', 'Repair fail'],
            ['repair_skipped_unsupported', 'Repair n/a'],
            ['pruned', 'Pruned'],
            ['prune_skipped_not_eligible', 'Prune skipped'],
            ['regrabbed', 'Re-grabbed'],
            ['regrab_failed', 'Re-grab fail'],
            ['regrab_skipped_no_arr_link', 'Re-grab no arr'],
            ['deletions', 'Deletions'],
            ['deletion_cap_skipped', 'Cap-skipped'],
        ];
        container.innerHTML = '';
        for (const [k, label] of fields) {
            const el = document.createElement('div');
            el.className = 'bg-base-100 rounded p-2';
            const title = RepairManager.STAT_TITLES[k];
            if (title) el.setAttribute('title', title);
            el.innerHTML = `<div class="text-[10px] opacity-60 uppercase">${label}</div><div class="font-mono">${stats[k] || 0}</div>`;
            container.appendChild(el);
        }
    }

    healthColor(status) {
        switch (status) {
            case 'healthy':
                return 'text-success';
            case 'broken':
                return 'text-error';
            case 'repairing':
                return 'text-info';
            case 'stale':
                return 'text-warning';
            case 'unsupported':
                return 'text-base-content/60';
            default:
                return '';
        }
    }

    async loadHistory() {
        try {
            const runs = await this.fetchJSON(`${this.api}/repair/runs`);
            this.renderHistory(runs || []);
        } catch (e) {
            console.error('Failed to load history', e);
        }
    }

    async loadBroken() {
        try {
            const list = await this.fetchJSON(`${this.api}/repair/health?status=broken`);
            this.renderBroken(list || []);
        } catch (e) {
            console.error('Failed to load broken entries', e);
        }
    }

    renderBroken(entries) {
        this.updateBrokenCount(entries.length);

        // Sort: most recently failed first, then by name.
        entries.sort((a, b) => {
            const ta = a.last_failed_at ? new Date(a.last_failed_at).getTime() : 0;
            const tb = b.last_failed_at ? new Date(b.last_failed_at).getTime() : 0;
            if (ta !== tb) return tb - ta;
            return (a.entry_name || '').localeCompare(b.entry_name || '');
        });

        this.brokenState.items = entries;
        // Clamp current page so a shrinking list doesn't strand the user on an empty page.
        const totalPages = Math.max(1, Math.ceil(entries.length / this.brokenState.pageSize));
        if (this.brokenState.page > totalPages) this.brokenState.page = totalPages;
        if (this.brokenState.page < 1) this.brokenState.page = 1;
        this.renderBrokenPage();
    }

    renderBrokenPage() {
        const tbody = document.getElementById('brokenTableBody');
        const empty = document.getElementById('noBrokenMessage');
        if (!tbody) return;
        tbody.innerHTML = '';

        const {items, page, pageSize} = this.brokenState;
        if (!items.length) {
            empty?.classList.remove('hidden');
            this.renderBrokenPagination();
            return;
        }
        empty?.classList.add('hidden');

        const start = (page - 1) * pageSize;
        const slice = items.slice(start, start + pageSize);

        for (const h of slice) {
            const rowId = `broken-row-${this.slug(h.entry_name)}`;
            const fileCount = h.file_count ?? 0;
            const brokenCount = h.broken_count ?? (h.broken_files?.length ?? 0);
            const lastChecked = h.last_checked_at ? new Date(h.last_checked_at).toLocaleString() : '-';
            const lastRepair = h.last_repair_at ? new Date(h.last_repair_at).toLocaleString() : '-';
            const reason = h.failure_reason || '-';

            const tr = document.createElement('tr');
            tr.className = 'cursor-pointer hover:bg-base-200';
            tr.innerHTML = `
                <td class="w-8">
                    <i class="bi bi-chevron-right transition-transform" id="${rowId}-caret"></i>
                </td>
                <td class="font-mono text-sm break-all">${this.escape(h.entry_name)}</td>
                <td><span class="badge badge-ghost badge-sm">${this.escape(h.protocol || 'unknown')}</span></td>
                <td>${fileCount}</td>
                <td class="text-error font-medium">${brokenCount}</td>
                <td class="text-xs">${this.escape(reason)}</td>
                <td class="text-xs">${lastChecked}</td>
                <td class="text-xs">${lastRepair}</td>
                <td class="text-right whitespace-nowrap">
                    <div class="inline-flex gap-1">
                        <button class="btn btn-xs btn-outline" data-action="recheck" data-name="${this.escapeAttr(h.entry_name)}" title="Recheck (CHECK only)" aria-label="Recheck ${this.escape(h.entry_name)}">
                            <i class="bi bi-search-heart"></i>
                        </button>
                        <button class="btn btn-xs btn-info btn-outline font-semibold" data-action="fix" data-component="repair" data-name="${this.escapeAttr(h.entry_name)}" title="REPAIR — re-acquire on same/backup provider" aria-label="Repair ${this.escape(h.entry_name)}">R</button>
                        <button class="btn btn-xs btn-warning btn-outline font-semibold" data-action="fix" data-component="prune" data-name="${this.escapeAttr(h.entry_name)}" title="PRUNE — delete decypharr-side, arr keeps monitoring" aria-label="Prune ${this.escape(h.entry_name)}">P</button>
                        <button class="btn btn-xs btn-error btn-outline font-semibold" data-action="fix" data-component="regrab" data-name="${this.escapeAttr(h.entry_name)}" title="RE-GRAB — arr delete + blocklist + search" aria-label="Re-grab ${this.escape(h.entry_name)}">G</button>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);

            const detail = document.createElement('tr');
            detail.id = rowId;
            detail.className = 'hidden';
            detail.innerHTML = `
                <td colspan="9" class="bg-base-200/40 p-0">
                    <div class="p-4 space-y-2">
                        ${this.renderEntryDiagnostics(h)}
                        ${this.renderBrokenFiles(h.broken_files || [])}
                    </div>
                </td>
            `;
            tbody.appendChild(detail);

            tr.addEventListener('click', (ev) => {
                if (ev.target.closest('[data-action]')) return;
                const hidden = detail.classList.toggle('hidden');
                const caret = document.getElementById(`${rowId}-caret`);
                if (caret) caret.style.transform = hidden ? '' : 'rotate(90deg)';
            });
            tr.querySelector('[data-action="recheck"]')?.addEventListener('click', (ev) => {
                ev.stopPropagation();
                this.recheckOne(h.entry_name);
            });
            tr.querySelectorAll('[data-action="fix"]').forEach((btn) => {
                btn.addEventListener('click', (ev) => {
                    ev.stopPropagation();
                    this.fixOne(h.entry_name, btn.getAttribute('data-component'));
                });
            });
        }
        this.renderBrokenPagination();
    }

    // renderEntryDiagnostics explains, per entry, why the last run's components
    // did not fix it: last_repair_error is a REPAIR attempt that FAILED, while
    // action_skips holds the per-component reason a component DECLINED to act
    // (a decline is not a failure). Both are omitted entirely when absent, so
    // the expanded row keeps its current shape for healthy-path entries.
    renderEntryDiagnostics(h) {
        const rows = [];
        if (h.last_repair_error) {
            rows.push(`<div><span class="font-semibold text-error">REPAIR failed:</span> ${this.escape(h.last_repair_error)}</div>`);
        }
        const skips = h.action_skips || {};
        const labels = {repair: 'REPAIR', prune: 'PRUNE', regrab: 'RE-GRAB'};
        for (const key of ['repair', 'prune', 'regrab']) {
            if (!skips[key]) continue;
            rows.push(`<div><span class="font-semibold text-warning">${labels[key]} declined:</span> ${this.escape(skips[key])}</div>`);
        }
        if (!rows.length) return '';
        return `<div class="rounded bg-base-100 p-3 text-xs space-y-1">${rows.join('')}</div>`;
    }

    renderBrokenPagination() {
        const bar = document.getElementById('brokenPaginationBar');
        const info = document.getElementById('brokenPaginationInfo');
        const controls = document.getElementById('brokenPaginationControls');
        if (!bar || !info || !controls) return;

        const {items, page, pageSize} = this.brokenState;
        const total = items.length;
        if (total === 0) {
            bar.classList.add('hidden');
            return;
        }
        bar.classList.remove('hidden');

        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        const start = (page - 1) * pageSize + 1;
        const end = Math.min(start + pageSize - 1, total);
        info.textContent = `Showing ${start}-${end} of ${total}`;

        if (totalPages <= 1) {
            controls.innerHTML = '';
            return;
        }

        let html = `<button class="join-item btn btn-sm ${page === 1 ? 'btn-disabled' : ''}"
                            onclick="window.repairManager.goToBrokenPage(${page - 1})">«</button>`;
        for (let i = 1; i <= totalPages; i++) {
            if (i === 1 || i === totalPages || (i >= page - 2 && i <= page + 2)) {
                html += `<button class="join-item btn btn-sm ${i === page ? 'btn-active' : ''}"
                                onclick="window.repairManager.goToBrokenPage(${i})">${i}</button>`;
            } else if (i === page - 3 || i === page + 3) {
                html += `<button class="join-item btn btn-sm btn-disabled">…</button>`;
            }
        }
        html += `<button class="join-item btn btn-sm ${page === totalPages ? 'btn-disabled' : ''}"
                         onclick="window.repairManager.goToBrokenPage(${page + 1})">»</button>`;
        controls.innerHTML = html;
    }

    goToBrokenPage(p) {
        const totalPages = Math.max(1, Math.ceil(this.brokenState.items.length / this.brokenState.pageSize));
        if (p < 1 || p > totalPages || p === this.brokenState.page) return;
        this.brokenState.page = p;
        this.renderBrokenPage();
    }

    renderBrokenFiles(files) {
        if (!files.length) {
            return `<div class="text-sm opacity-60">No broken file details.</div>`;
        }
        const rows = files.map(f => {
            const arr = f.arr_name ? `${this.escape(f.arr_name)}${f.arr_kind ? ` (${this.escape(f.arr_kind)})` : ''}` : '<span class="opacity-50">—</span>';
            const ids = [];
            if (f.media_id) ids.push(`media:${f.media_id}`);
            if (f.episode_id) ids.push(`ep:${f.episode_id}`);
            if (f.arr_file_id) ids.push(`file:${f.arr_file_id}`);
            const idStr = ids.length ? `<span class="font-mono text-[10px] opacity-70">${ids.join(' · ')}</span>` : '';
            const size = f.size ? this.formatBytes(f.size) : '-';
            return `
                <tr>
                    <td class="font-mono text-xs break-all">${this.escape(f.file_name || '')}</td>
                    <td class="text-xs">${this.escape(f.reason || '-')}</td>
                    <td class="text-xs">${size}</td>
                    <td class="text-xs">${arr}</td>
                    <td>${idStr}</td>
                </tr>
            `;
        }).join('');
        return `
            <div class="overflow-x-auto">
                <table class="table table-xs">
                    <thead><tr><th>File</th><th>Reason</th><th>Size</th><th>Arr</th><th>Ids</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    async recheckOne(name) {
        try {
            const res = await fetch(`${this.api}/repair/health/${encodeURIComponent(name)}/check`, {method: 'POST'});
            if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
            this.toast(`Recheck started for ${name}`, 'success');
            // Recheck flips the entry to repairing; refresh shortly so the row updates.
            setTimeout(() => { this.loadBroken(); this.loadStatus(); }, 800);
        } catch (e) {
            this.toast(`Recheck failed: ${e.message}`, 'error');
        }
    }

    async fixOne(name, component) {
        const meta = this.fixComponentMeta(component);
        if (!confirm(`Run ${meta.label} on "${name}"?\n\n${meta.label} = ${meta.desc}.`)) return;
        try {
            const res = await fetch(`${this.api}/repair/fix`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({names: [name], actions: {[component]: true}}),
            });
            if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
            this.toast(`${meta.label} started for ${name}`, 'success');
            window.location.reload();
        } catch (e) {
            this.toast(`${meta.label} failed: ${e.message}`, 'error');
        }
    }

    slug(s) {
        return String(s || '').replace(/[^a-zA-Z0-9_-]+/g, '_');
    }

    escapeAttr(s) {
        return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    formatBytes(n) {
        if (!n || n < 0) return '-';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let i = 0;
        let v = n;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }

    renderHistory(runs) {
        const tbody = document.getElementById('runsTableBody');
        const empty = document.getElementById('noRunsMessage');
        tbody.innerHTML = '';
        if (!runs.length) {
            empty.classList.remove('hidden');
            return;
        }
        empty.classList.add('hidden');
        for (const run of runs) {
            const tr = document.createElement('tr');
            const start = run.started_at ? new Date(run.started_at) : null;
            const end = run.completed_at ? new Date(run.completed_at) : null;
            const duration = start && end ? this.formatDuration(end - start) : (start ? 'running' : '-');
            const s = run.stats || {};
            const deletions = s.deletions ?? 0;
            const capSkipped = s.deletion_cap_skipped ?? 0;
            const deletionsCell = capSkipped
                ? `${deletions} <span class="text-warning" title="left un-deleted by the per-run cap">(+${capSkipped})</span>`
                : `${deletions}`;
            // Each component's failures and principled declines ride along in
            // its own column, following the Deletions cell's "(+N)" precedent:
            // a bare "Repaired: 0" cannot tell "REPAIR is broken" apart from
            // "every dead entry was an nzb, which REPAIR cannot re-acquire".
            const annotate = (key, cls, label) => {
                const n = s[key] ?? 0;
                if (!n) return '';
                return ` <span class="${cls} text-xs whitespace-nowrap" title="${RepairManager.STAT_TITLES[key]}">(${label} ${n})</span>`;
            };
            const repairedCell = `${s.reacquired ?? 0}`
                + annotate('repair_failed', 'text-error', 'fail')
                + annotate('repair_skipped_unsupported', 'text-warning', 'n/a');
            const prunedCell = `${s.pruned ?? 0}`
                + annotate('prune_skipped_not_eligible', 'text-warning', 'skipped');
            const regrabbedCell = `${s.regrabbed ?? 0}`
                + annotate('regrab_failed', 'text-error', 'fail')
                + annotate('regrab_skipped_no_arr_link', 'text-warning', 'no arr');
            tr.innerHTML = `
                <td class="font-mono text-sm">${start ? start.toLocaleString() : '-'}</td>
                <td>${this.escape(run.trigger || '-')}</td>
                <td>${this.statusBadge(run.status)}</td>
                <td>${s.probed ?? 0}</td>
                <td class="${s.broken ? 'text-error font-medium' : ''}">${s.broken ?? 0}</td>
                <td class="${s.reacquired ? 'text-success font-medium' : ''}">${repairedCell}</td>
                <td class="${s.pruned ? 'text-warning font-medium' : ''}">${prunedCell}</td>
                <td class="${s.regrabbed ? 'text-info font-medium' : ''}">${regrabbedCell}</td>
                <td class="${deletions ? 'font-medium' : ''}">${deletionsCell}</td>
                <td>${duration}</td>
                <td class="text-xs text-error">${this.escape(run.error || '')}</td>
            `;
            tbody.appendChild(tr);
        }
    }

    statusBadge(status) {
        const cls = {
            running: 'badge-info',
            completed: 'badge-success',
            failed: 'badge-error',
            cancelled: 'badge-warning',
        }[status] || 'badge-ghost';
        return `<span class="badge ${cls}">${this.escape(status || 'unknown')}</span>`;
    }

    formatDuration(ms) {
        if (!ms || ms < 0) return '-';
        const s = Math.round(ms / 1000);
        if (s < 60) return `${s}s`;
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}m ${r}s`;
    }

    async fetchJSON(url) {
        const res = await fetch(url, {credentials: 'same-origin'});
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
    }

    toast(message, type = 'info') {
        // common.js exposes the shared toast as window.createToast; the older
        // names are kept only as fallbacks. Without this the repair page's
        // toasts silently went to console.log and the user saw nothing.
        // Pass the message RAW: createToast escapes it itself now (it is the
        // single place that escapes), and escaping here too would render
        // literal `&lt;` artifacts to the user.
        if (typeof window.createToast === 'function') return window.createToast(message, type);
        if (typeof window.toast === 'function') return window.toast(message, type);
        if (typeof window.showToast === 'function') return window.showToast(message, type);
        console.log(`[${type}]`, message);
    }
}

// Human-readable explanations for the failure / decline counters, shared by
// every surface that renders run stats so the wording can never drift apart.
//
// The three *skipped* counters exist because a component that DECLINES to act
// is otherwise indistinguishable from one that silently broke: a run reporting
// "repaired: 0" reads as "REPAIR is broken" when the truth may be "REPAIR
// correctly refused — every dead entry was an nzb, which cannot be
// re-acquired". They are rendered as visible counts, not hidden behind a
// tooltip; the tooltip only carries the longer explanation.
//
// These strings are static literals — they are interpolated into HTML
// attributes and must stay free of quotes and markup.
RepairManager.STAT_TITLES = {
    repair_failed: 'REPAIR: re-acquire attempts that errored',
    repair_skipped_unsupported: 'REPAIR declined: the entry protocol cannot be re-acquired (nzb entries can only be RE-GRABbed or PRUNEd)',
    prune_skipped_not_eligible: 'PRUNE declined: only some files in the entry are broken, so deleting the whole entry would be wrong',
    regrab_failed: 'RE-GRAB: arr-side failures (file delete / blocklist / search)',
    regrab_skipped_no_arr_link: 'RE-GRAB declined: the entry has no resolved arr link to route through',
};

window.RepairManager = RepairManager;

(function () {
  'use strict';

  const center = document.getElementById('backgroundExportCenter');
  if (!center) return;

  const list = center.querySelector('.background-export-list');
  const toggle = center.querySelector('.background-export-toggle');
  const clearButton = center.querySelector('.background-export-clear');
  const labels = center.dataset;
  const jobs = new Map();
  const dismissedKey = 'dataorcid-dismissed-export-jobs';
  let timer = null;
  let loading = false;
  const terminalStatuses = ['success', 'failed', 'partial', 'interrupted'];

  function isTerminal(job) {
    return terminalStatuses.includes(job.status);
  }

  function dismissedJobs() {
    try {
      return new Set(JSON.parse(window.localStorage.getItem(dismissedKey) || '[]'));
    } catch (_error) {
      return new Set();
    }
  }

  function storeDismissed(values) {
    window.localStorage.setItem(dismissedKey, JSON.stringify(Array.from(values).slice(-50)));
  }

  function statusCopy(job) {
    if (job.status === 'success') {
      return job.reused ? labels.reusedLabel : labels.readyLabel;
    }
    if (['failed', 'partial', 'interrupted'].includes(job.status)) return labels.failedLabel;
    if (job.status === 'running') return labels.runningLabel;
    return labels.queuedLabel;
  }

  function formatSize(bytes) {
    if (!bytes) return '';
    const units = ['B', 'KB', 'MB', 'GB'];
    let value = Number(bytes);
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit += 1;
    }
    return `${value.toFixed(unit ? 1 : 0)} ${units[unit]}`;
  }

  function render() {
    const dismissed = dismissedJobs();
    const allJobs = Array.from(jobs.values());
    const terminal = allJobs.filter(isTerminal);
    const visible = allJobs.filter((job) => !dismissed.has(job.id));
    clearButton.hidden = terminal.length === 0;
    center.hidden = visible.length === 0 && terminal.length === 0;
    list.replaceChildren();

    visible.forEach((job) => {
      const terminal = isTerminal(job);
      const item = document.createElement('article');
      item.className = `background-export-item is-${job.status}`;

      const icon = document.createElement('span');
      icon.className = 'background-export-icon';
      const iconElement = document.createElement('i');
      iconElement.className = job.status === 'success'
        ? 'fas fa-check-circle'
        : terminal
          ? 'fas fa-exclamation-circle'
          : 'fas fa-circle-notch fa-spin';
      icon.appendChild(iconElement);

      const body = document.createElement('div');
      body.className = 'background-export-body';
      const title = document.createElement('strong');
      title.textContent = job.label || labels.preparingLabel;
      const status = document.createElement('span');
      status.textContent = statusCopy(job);
      body.append(title, status);

      if (job.status === 'running' && job.total > 0) {
        const progress = document.createElement('div');
        progress.className = 'background-export-progress';
        const bar = document.createElement('span');
        bar.style.width = `${Math.min(100, Math.round((job.current / job.total) * 100))}%`;
        progress.appendChild(bar);
        body.appendChild(progress);
      }

      if (job.status === 'success') {
        const meta = document.createElement('small');
        const details = [];
        if (job.records) details.push(`${Number(job.records).toLocaleString()} ${labels.recordsLabel}`);
        if (job.size_bytes) details.push(formatSize(job.size_bytes));
        meta.textContent = details.join(' · ');
        if (meta.textContent) body.appendChild(meta);

        const download = document.createElement('a');
        download.className = 'btn btn-sm btn-primary background-export-download';
        download.href = job.download_url;
        download.innerHTML = '<i class="fas fa-download" aria-hidden="true"></i>';
        download.append(` ${labels.downloadLabel}`);
        body.appendChild(download);
      }

      const dismiss = document.createElement('button');
      dismiss.type = 'button';
      dismiss.className = 'background-export-dismiss';
      dismiss.setAttribute('aria-label', labels.closeLabel);
      dismiss.innerHTML = '<i class="fas fa-times" aria-hidden="true"></i>';
      dismiss.addEventListener('click', function () {
        const values = dismissedJobs();
        values.add(job.id);
        storeDismissed(values);
        render();
      });

      item.append(icon, body, dismiss);
      list.appendChild(item);
    });
  }

  function merge(payload) {
    if (Array.isArray(payload.jobs)) {
      const serverIds = new Set(payload.jobs.map((job) => job.id));
      Array.from(jobs.keys()).forEach((id) => {
        if (!id.startsWith('request-') && !id.startsWith('clear-request-') && !serverIds.has(id)) {
          jobs.delete(id);
        }
      });
    }
    if (payload.reused && payload.job) payload.job.reused = true;
    (payload.jobs || (payload.job ? [payload.job] : [])).forEach((job) => {
      const previous = jobs.get(job.id);
      jobs.set(job.id, {
        ...job,
        reused: Boolean(job.reused || (previous && previous.reused))
      });
    });
    render();
  }

  async function refresh() {
    if (loading) return;
    loading = true;
    try {
      const response = await window.fetch(center.dataset.listUrl, {
        headers: {'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest'},
        credentials: 'same-origin'
      });
      if (response.ok) merge(await response.json());
    } catch (_error) {
      // Keep the last known state; a later poll can reconnect transparently.
    } finally {
      loading = false;
      const active = Array.from(jobs.values()).some((job) => ['queued', 'running'].includes(job.status));
      window.clearTimeout(timer);
      timer = window.setTimeout(refresh, active ? 3000 : 20000);
    }
  }

  async function requestExport(anchor) {
    const url = new URL(anchor.href, window.location.origin);
    url.searchParams.set('background', '1');
    anchor.classList.add('is-loading');
    anchor.setAttribute('aria-disabled', 'true');
    center.hidden = false;
    try {
      const response = await window.fetch(url.toString(), {
        headers: {'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest'},
        credentials: 'same-origin'
      });
      const contentType = response.headers.get('content-type') || '';
      if (!response.ok || !contentType.includes('application/json')) throw new Error('export request failed');
      const payload = await response.json();
      if (!payload.job) throw new Error('missing export job');
      const dismissed = dismissedJobs();
      dismissed.delete(payload.job.id);
      storeDismissed(dismissed);
      merge(payload);
      center.classList.remove('is-collapsed');
      toggle.setAttribute('aria-expanded', 'true');
      refresh();
    } catch (_error) {
      const id = `request-${Date.now()}`;
      jobs.set(id, {
        id: id,
        label: labels.requestFailedLabel,
        status: 'failed'
      });
      render();
    } finally {
      anchor.classList.remove('is-loading');
      anchor.removeAttribute('aria-disabled');
    }
  }

  async function clearExports() {
    if (!window.confirm(labels.clearConfirmLabel)) return;
    const original = clearButton.innerHTML;
    clearButton.disabled = true;
    clearButton.innerHTML = '<i class="fas fa-circle-notch fa-spin" aria-hidden="true"></i>';
    clearButton.append(` ${labels.clearingLabel}`);
    try {
      const response = await window.fetch(labels.clearUrl, {
        method: 'POST',
        headers: {
          'Accept': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
          'X-CSRFToken': labels.csrfToken
        },
        credentials: 'same-origin'
      });
      if (!response.ok) throw new Error('export cleanup failed');
      const dismissed = dismissedJobs();
      Array.from(jobs.entries()).forEach(([id, job]) => {
        if (!isTerminal(job)) return;
        jobs.delete(id);
        dismissed.delete(id);
      });
      storeDismissed(dismissed);
      render();
    } catch (_error) {
      const id = `clear-request-${Date.now()}`;
      jobs.set(id, {id: id, label: labels.requestFailedLabel, status: 'failed'});
      render();
    } finally {
      clearButton.disabled = false;
      clearButton.innerHTML = original;
    }
  }

  document.addEventListener('click', function (event) {
    const anchor = event.target.closest('a.js-background-export, a.js-large-export');
    if (!anchor || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    requestExport(anchor);
  });

  toggle.addEventListener('click', function () {
    const collapsed = center.classList.toggle('is-collapsed');
    toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    toggle.querySelector('i').className = collapsed ? 'fas fa-chevron-up' : 'fas fa-chevron-down';
  });

  clearButton.addEventListener('click', clearExports);

  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) refresh();
  });
  refresh();
}());

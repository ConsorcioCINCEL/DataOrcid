(function () {
  'use strict';

  function safeFilename(value) {
    return (value || 'chart')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'chart';
  }

  function chartFilename(button) {
    const card = button.closest('.card, .dashboard-panel');
    const heading = card && card.querySelector('.card-title, h2');
    return safeFilename(button.dataset.filename || (heading && heading.textContent));
  }

  function renderedCanvas(source) {
    const output = document.createElement('canvas');
    output.width = source.width;
    output.height = source.height;
    const context = output.getContext('2d');
    context.fillStyle = '#ffffff';
    context.fillRect(0, 0, output.width, output.height);
    context.drawImage(source, 0, 0);
    return output;
  }

  function downloadUrl(url, filename) {
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
  }

  function exportPng(canvas, filename) {
    downloadUrl(renderedCanvas(canvas).toDataURL('image/png'), filename + '.png');
  }

  let plotlyLoader;

  function loadPlotly() {
    if (window.Plotly) return Promise.resolve(window.Plotly);
    if (plotlyLoader) return plotlyLoader;

    plotlyLoader = new Promise(function (resolve, reject) {
      const script = document.createElement('script');
      script.src = 'https://cdn.jsdelivr.net/npm/plotly.js-basic-dist-min@3.7.0/plotly-basic.min.js';
      script.async = true;
      script.onload = function () { resolve(window.Plotly); };
      script.onerror = function () { reject(new Error('Could not load the SVG export renderer.')); };
      document.head.appendChild(script);
    });
    return plotlyLoader;
  }

  function colorValue(value, index, fallback) {
    if (Array.isArray(value)) return value[index % value.length] || fallback;
    return value || fallback;
  }

  function plotlySpec(chart, title) {
    const labels = chart.data.labels || [];
    const chartType = chart.config.type;
    const datasets = chart.data.datasets || [];
    const palette = ['#2a69b8', '#28a745', '#f39c12', '#dc3545', '#17a2b8', '#6f42c1'];
    let hasSecondaryAxis = false;
    let horizontal = false;
    let traces;

    if (chartType === 'pie' || chartType === 'doughnut') {
      const dataset = datasets[0] || { data: [] };
      traces = [{
        type: 'pie',
        labels: labels,
        values: dataset.data,
        hole: chartType === 'doughnut' ? 0.52 : 0,
        marker: {
          colors: labels.map(function (_label, index) {
            return colorValue(dataset.backgroundColor, index, palette[index % palette.length]);
          })
        },
        textinfo: 'percent',
        hoverinfo: 'label+value+percent'
      }];
    } else {
      horizontal = chart.options && chart.options.indexAxis === 'y';
      traces = datasets.map(function (dataset, datasetIndex) {
        const type = dataset.type || chartType;
        const color = colorValue(
          dataset.borderColor || dataset.backgroundColor,
          datasetIndex,
          palette[datasetIndex % palette.length]
        );
        const trace = {
          name: dataset.label || '',
          x: horizontal ? dataset.data : labels,
          y: horizontal ? labels : dataset.data,
          yaxis: dataset.yAxisID === 'y1' ? 'y2' : 'y'
        };
        if (dataset.yAxisID === 'y1') hasSecondaryAxis = true;

        if (type === 'line') {
          trace.type = 'scatter';
          trace.mode = 'lines+markers';
          trace.line = { color: color, width: dataset.borderWidth || 2 };
          trace.marker = { color: color, size: 5 };
          if (dataset.fill) {
            trace.fill = 'tozeroy';
            trace.fillcolor = colorValue(dataset.backgroundColor, datasetIndex, 'rgba(42,105,184,0.10)');
          }
        } else {
          trace.type = 'bar';
          trace.orientation = horizontal ? 'h' : 'v';
          trace.marker = { color: dataset.backgroundColor || color };
        }
        return trace;
      });
    }

    const layout = {
      autosize: false,
      barmode: 'group',
      font: { family: 'Source Sans 3, sans-serif', color: '#5d6875', size: 12 },
      height: chart.height,
      legend: { orientation: 'h', x: 0, y: -0.18 },
      margin: { l: 70, r: hasSecondaryAxis ? 70 : 30, t: 62, b: 75 },
      paper_bgcolor: '#ffffff',
      plot_bgcolor: '#ffffff',
      showlegend: traces.length > 1 || chartType === 'pie' || chartType === 'doughnut',
      title: { text: title, x: 0.01, xanchor: 'left', font: { size: 17, color: '#152536' } },
      width: chart.width,
      xaxis: {
        automargin: true,
        gridcolor: '#e7ebf0',
        linecolor: '#d7dde4',
        type: horizontal ? 'linear' : 'category',
        zeroline: false
      },
      yaxis: {
        automargin: true,
        gridcolor: '#e7ebf0',
        linecolor: '#d7dde4',
        type: horizontal ? 'category' : 'linear',
        zeroline: false
      }
    };
    if (hasSecondaryAxis) {
      layout.yaxis2 = {
        automargin: true,
        overlaying: 'y',
        showgrid: false,
        side: 'right',
        zeroline: false
      };
    }
    return { layout: layout, traces: traces };
  }

  async function exportSvg(canvas, filename, title) {
    const chart = window.Chart && window.Chart.getChart(canvas);
    if (!chart) throw new Error('The chart is not ready for SVG export.');

    const Plotly = await loadPlotly();
    const spec = plotlySpec(chart, title);
    const target = document.createElement('div');
    target.style.cssText = 'left:-10000px;position:fixed;top:0;visibility:hidden;';
    document.body.appendChild(target);
    try {
      await Plotly.newPlot(target, spec.traces, spec.layout, { displayModeBar: false, staticPlot: true });
      const dataUrl = await Plotly.toImage(target, {
        format: 'svg',
        height: chart.height,
        width: chart.width
      });
      downloadUrl(dataUrl, filename + '.svg');
    } finally {
      Plotly.purge(target);
      target.remove();
    }
  }

  document.addEventListener('click', async function (event) {
    const button = event.target.closest('.js-chart-export');
    if (!button) return;

    const canvas = document.getElementById(button.dataset.chartId);
    if (!canvas || canvas.tagName !== 'CANVAS' || !canvas.width || !canvas.height) return;

    const filename = chartFilename(button);
    const card = button.closest('.card, .dashboard-panel');
    const heading = card && card.querySelector('.card-title, h2');
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
    try {
      if (button.dataset.chartFormat === 'svg') {
        await exportSvg(canvas, filename, heading ? heading.textContent.trim() : filename);
      } else {
        exportPng(canvas, filename);
      }
    } finally {
      button.disabled = false;
      button.removeAttribute('aria-busy');
    }
  });
})();

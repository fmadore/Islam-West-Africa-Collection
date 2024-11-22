class TreemapVisualization {
    constructor(config) {
        this.container = config.container;
        this.width = config.width || this.container.clientWidth;
        this.height = config.height || this.container.clientHeight;
        this.language = config.language || 'en';
        this.translations = config.translations;
        this.onZoomChange = config.onZoomChange || (() => {});
        
        this.svg = null;
        this.tooltip = null;
        this.root = null;
        this.focus = null;
        this.view = null;
        
        this.initializeVisualization();
    }

    initializeVisualization() {
        // Create SVG
        this.svg = d3.select(this.container)
            .append("svg")
            .attr("width", this.width)
            .attr("height", this.height)
            .attr("viewBox", `0 0 ${this.width} ${this.height}`)
            .attr("preserveAspectRatio", "xMidYMid meet");

        // Create tooltip
        this.tooltip = d3.select("body")
            .append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);

        // Create main group
        this.g = this.svg.append("g");
    }

    createTreemap(data) {
        const treemap = d3.treemap()
            .size([this.width, this.height])
            .paddingTop(20)
            .paddingRight(2)
            .paddingBottom(2)
            .paddingLeft(2)
            .paddingInner(3)
            .round(true);

        this.root = d3.hierarchy(data)
            .sum(d => d.value)
            .sort((a, b) => b.value - a.value);

        treemap(this.root);
        this.focus = this.root;
        
        this.renderCells();
        this.zoomTo([this.root.x0, this.root.y0, this.root.x1 - this.root.x0]);
    }

    renderCells() {
        const cell = this.g.selectAll("g")
            .data(this.root.descendants())
            .join("g")
            .attr("transform", d => `translate(${d.x0},${d.y0})`);

        this.renderRectangles(cell);
        this.renderLabels(cell);
        this.setupInteractions(cell);
    }

    renderRectangles(cell) {
        return cell.append("rect")
            .attr("class", "node")
            .attr("width", d => d.x1 - d.x0)
            .attr("height", d => d.y1 - d.y0)
            .attr("fill", d => d.data.color)
            .attr("opacity", d => d.depth === 1 ? 0.85 : 0.75);
    }

    renderLabels(cell) {
        const self = this;

        cell.each(function(d) {
            const el = d3.select(this);
            const width = d.x1 - d.x0;
            const height = d.y1 - d.y0;

            if (d.depth === 1) {
                el.append("text")
                    .attr("class", "country-label")
                    .attr("x", width / 2)
                    .attr("y", height * 0.15)
                    .attr("text-anchor", "middle")
                    .text(d.data.name);

                const percentage = (d.value / self.root.value * 100).toFixed(1);
                el.append("text")
                    .attr("class", "percentage-label")
                    .attr("x", width / 2)
                    .attr("y", height * 0.3)
                    .attr("text-anchor", "middle")
                    .text(`${percentage}%`);

            } else if (d.depth === 2) {
                if (width > 50 && height > 30) {
                    const maxLength = Math.floor(width / 10);
                    const displayName = self.truncateText(d.data.name, maxLength);
                    const percentage = (d.value / d.parent.value * 100).toFixed(1);
                    
                    const fontSize = Math.min(
                        Math.floor(width / 10),
                        Math.floor(height / 4),
                        14
                    );

                    if (fontSize >= 8) {
                        el.append("text")
                            .attr("class", "item-label")
                            .attr("x", width / 2)
                            .attr("y", height / 2 - fontSize/2)
                            .attr("text-anchor", "middle")
                            .style("font-size", `${fontSize}px`)
                            .text(displayName);

                        el.append("text")
                            .attr("class", "percentage-label")
                            .attr("x", width / 2)
                            .attr("y", height / 2 + fontSize/2)
                            .attr("text-anchor", "middle")
                            .style("font-size", `${fontSize}px`)
                            .text(`${percentage}%`);
                    }
                }
            }
        });
    }

    setupInteractions(cell) {
        cell.style("cursor", "pointer")
            .on("click", (event, d) => this.zoom(event, d))
            .on("mouseover", (event, d) => this.showTooltip(event, d))
            .on("mouseout", () => this.hideTooltip());
    }

    zoom(event, d) {
        if (this.focus !== d) {
            this.focus = d;
            this.transition(d);
            this.onZoomChange(true);
        } else {
            this.focus = this.root;
            this.transition(this.root);
            this.onZoomChange(false);
        }
    }

    transition(d) {
        const transition = this.g.transition()
            .duration(750)
            .tween("zoom", () => {
                const i = d3.interpolateZoom(this.view, [d.x0, d.y0, d.x1 - d.x0]);
                return t => this.zoomTo(i(t));
            });
    }

    zoomTo(v) {
        const k = this.width / v[2];
        this.view = v;

        this.g.selectAll("g")
            .attr("transform", d => `translate(${(d.x0 - v[0]) * k},${(d.y0 - v[1]) * k})`);

        this.g.selectAll("rect")
            .attr("width", d => Math.max(0, (d.x1 - d.x0) * k))
            .attr("height", d => Math.max(0, (d.y1 - d.y0) * k));

        this.updateTextSizes(k);
    }

    updateTextSizes(k) {
        this.g.selectAll(".country-label")
            .style("font-size", d => {
                const width = (d.x1 - d.x0) * k;
                return `${Math.min(26, Math.max(16, width / 20))}px`;
            });
        
        this.g.selectAll(".item-label, .percentage-label")
            .style("font-size", d => {
                const width = (d.x1 - d.x0) * k;
                const height = (d.y1 - d.y0) * k;
                const fontSize = Math.min(
                    Math.floor(width / 10),
                    Math.floor(height / 4),
                    14
                );
                return `${Math.max(8, fontSize)}px`;
            });
    }

    showTooltip(event, d) {
        const percentage = (d.value / this.root.value * 100).toFixed(1);
        const itemCount = d.value.toLocaleString();
        
        let content = this.createTooltipContent(d, itemCount, percentage);
        
        this.tooltip.transition()
            .duration(200)
            .style("opacity", 0.9);
            
        this.tooltip.html(content)
            .style("left", `${Math.min(event.pageX + 10, window.innerWidth - 200)}px`)
            .style("top", `${Math.min(event.pageY - 28, window.innerHeight - 100)}px`);
    }

    createTooltipContent(d, itemCount, percentage) {
        if (d.parent && d.parent.data.name !== "root") {
            return `
                <strong>${d.parent.data.name}</strong><br/>
                ${d.data.name}<br/>
                ${this.translations[this.language].items}: ${itemCount}<br/>
                ${percentage}%
            `;
        }
        return `
            <strong>${d.data.name}</strong><br/>
            ${this.translations[this.language].items}: ${itemCount}<br/>
            ${percentage}%
        `;
    }

    hideTooltip() {
        this.tooltip.transition()
            .duration(500)
            .style("opacity", 0);
    }

    truncateText(text, maxLength) {
        if (!text) return '';
        return text.length > maxLength ? 
            text.slice(0, maxLength - 2) + '…' : 
            text;
    }

    resize() {
        this.width = this.container.clientWidth;
        this.height = this.container.clientHeight;
        
        this.svg
            .attr("width", this.width)
            .attr("height", this.height)
            .attr("viewBox", `0 0 ${this.width} ${this.height}`);

        if (this.root) {
            this.createTreemap(this.root.data);
        }
    }
} 
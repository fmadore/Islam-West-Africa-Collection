import json
import os
from pathlib import Path
from typing import Dict, DefaultDict
from collections import defaultdict
from Items_over_years import Config, DataFetcher

# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

class D3Visualizer:
    """Handles D3.js visualization generation."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def create_visualization(self, items_by_year_type: DefaultDict[str, DefaultDict[str, int]], language: str = 'en'):
        """Create and save the D3.js visualization."""
        # Create translation mapping for type names
        type_translations = {
            type_info['en']: type_info[language]
            for type_key, type_info in self.config.COLOR_PALETTE.items()
        }

        # Calculate total counts for each type using translated names
        type_totals = defaultdict(int)
        for year_data in items_by_year_type.values():
            for type_name, count in year_data.items():
                translated_name = type_translations.get(type_name, type_name)
                type_totals[translated_name] += count

        # Prepare data in the format D3 expects
        years = sorted(items_by_year_type.keys())
        types = sorted(type_totals.keys(), key=lambda x: -type_totals[x])
        
        # Prepare data using translated type names
        data = []
        for year in years:
            year_data = {"year": year}
            for t in types:
                translated_name = type_translations.get(t, t)
                year_data[translated_name] = items_by_year_type[year].get(t, 0)
            year_data["total"] = sum(items_by_year_type[year].values())
            data.append(year_data)

        # Create color mapping using translated names
        color_map = {
            type_info[language]: type_info["color"]
            for type_key, type_info in self.config.COLOR_PALETTE.items()
        }

        # Get labels based on language
        labels = self.config.LABELS[language]

        # Generate the HTML template with embedded D3 code
        html_content = self._generate_html_template(
            data=data,
            types=types,  # Use translated types
            color_map=color_map,
            labels=labels,
            language=language
        )

        # Save the visualization
        output_filename = f"item_distribution_over_years_{language}_d3.html"
        output_path = os.path.join(SCRIPT_DIR, output_filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

    def _generate_html_template(self, data, types, color_map, labels, language):
        """Generate the HTML template with embedded D3 code."""
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{labels['title']}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        .tooltip {{
            position: absolute;
            padding: 10px;
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            pointer-events: none;
            font-family: sans-serif;
            font-size: 14px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .total-line {{
            fill: none;
            stroke: rgba(0, 0, 0, 0.7);
            stroke-width: 2;
            stroke-dasharray: 4;
        }}
        .legend-item {{
            cursor: pointer;
        }}
        .legend-item:hover {{
            opacity: 0.7;
        }}
    </style>
</head>
<body>
    <div id="chart"></div>
    <script>
        // Data
        const data = {json.dumps(data)};
        const types = {json.dumps(types)};
        const colorMap = {json.dumps(color_map)};
        const labels = {json.dumps(labels)};

        // Set up dimensions
        const margin = {{top: 50, right: 60, bottom: 150, left: 80}};
        const width = 1200 - margin.left - margin.right;
        const height = 800 - margin.top - margin.bottom;

        // Create SVG
        const svg = d3.select("#chart")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${{margin.left}},${{margin.top}})`);

        // Set up scales
        const x = d3.scaleBand()
            .domain(data.map(d => d.year))
            .range([0, width])
            .padding(0.1);

        const y = d3.scaleLinear()
            .domain([0, d3.max(data, d => d.total)])
            .range([height, 0]);

        // Create tooltip
        const tooltip = d3.select("body")
            .append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);

        // Create stack generator
        const stack = d3.stack()
            .keys(types);

        const stackedData = stack(data);

        // Add bars
        const layers = svg.selectAll("g.layer")
            .data(stackedData)
            .join("g")
            .attr("class", "layer")
            .style("fill", (d, i) => colorMap[types[i]] || '#' + Math.random().toString(16).substr(-6));

        layers.selectAll("rect")
            .data(d => d)
            .join("rect")
            .attr("x", d => x(d.data.year))
            .attr("y", d => y(d[1]))
            .attr("height", d => y(d[0]) - y(d[1]))
            .attr("width", x.bandwidth())
            .on("mouseover", function(event, d) {{
                const type = d3.select(this.parentNode).datum().key;
                const value = d[1] - d[0];
                tooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                tooltip.html(`<b>${{type}}</b><br>${{value.toLocaleString()}} ${{labels.number_of_items}}`)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
            }})
            .on("mouseout", function(d) {{
                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0);
            }});

        // Add total line
        const line = d3.line()
            .x(d => x(d.year) + x.bandwidth() / 2)
            .y(d => y(d.total));

        svg.append("path")
            .datum(data)
            .attr("class", "total-line")
            .attr("d", line);

        // Add axes
        svg.append("g")
            .attr("transform", `translate(0,${{height}})`)
            .call(d3.axisBottom(x))
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-45)");

        svg.append("g")
            .call(d3.axisLeft(y).ticks(null, ",.0f"));

        // Add title
        svg.append("text")
            .attr("x", width / 2)
            .attr("y", -margin.top / 2)
            .attr("text-anchor", "middle")
            .style("font-size", "16px")
            .text(labels.title);

        // Add legend
        const legend = svg.append("g")
            .attr("font-family", "sans-serif")
            .attr("font-size", 10)
            .attr("text-anchor", "start")
            .selectAll("g")
            .data(types.concat(['Total']))
            .join("g")
            .attr("class", "legend-item")
            .attr("transform", (d, i) => `translate(0,${{height + margin.bottom - 20}})`);

        const legendColumns = 3;
        const legendWidth = width / legendColumns;
        
        legend.append("rect")
            .attr("x", (d, i) => (i % legendColumns) * legendWidth)
            .attr("y", (d, i) => Math.floor(i / legendColumns) * 20)
            .attr("width", 15)
            .attr("height", 15)
            .attr("fill", d => d === 'Total' ? "none" : (colorMap[d] || '#000'))
            .attr("stroke", d => d === 'Total' ? "rgba(0, 0, 0, 0.7)" : "none")
            .attr("stroke-dasharray", d => d === 'Total' ? "4" : "none");

        legend.append("text")
            .attr("x", (d, i) => (i % legendColumns) * legendWidth + 20)
            .attr("y", (d, i) => Math.floor(i / legendColumns) * 20 + 12)
            .text(d => d === 'Total' ? labels.total : d);
    </script>
</body>
</html>
"""

def main():
    """Main execution function."""
    try:
        config = Config()
        data_fetcher = DataFetcher(config)
        d3_visualizer = D3Visualizer(config)

        for language in ['en', 'fr']:
            print(f"Processing {language} visualization...")
            items_by_year_type = data_fetcher.fetch_items(language=language)
            d3_visualizer.create_visualization(items_by_year_type, language=language)
            print(f"Completed {language} visualization")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main() 
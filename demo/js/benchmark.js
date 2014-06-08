var data_bench_import_fix_frag = [
    [5000, 0.567],
    [10000, 1.159],
    [20000, 1.47],
    [40000, 2.62],
    [80000, 6.68],
    [120000, 8.78],
    [160000, 10.56],
    [200000, 11.98]
]

var data_bench_var_frag = [
    [10, 5.596],
    [11, 2.550],
    [12, 1.985],
    [13, 1.762],
    [14, 1.500],
    [15, 0.508],
    [16, 0.360]
]

var bench_var_coll_size = [
    {
        "label": "1k query",
        "data": [
            [5000, [15.67, 17.20, 16.03]],
            [10000, [18.03, 13.66, 14.95]],
            [20000, [16.13, 12.41, 17.83]],
            [40000, [15.08, 14.39, 15.81]],
            [80000, [13.21, 13.78, 18.21]],
            [120000, [16.25, 17.61, 14.76]],
            [160000, [14.59, 15.52, 13.79]],
            [200000, [16.21, 12.88, 15.17]]
        ]
    },
    {
        "label": "5k query",
        "data": [
            [40000, [59.44, 47.52, 41.16]],
            [80000, [58.96, 58.97, 45.45]],
            [120000, [44.24, 55.31, 51.89]],
            [160000, [49.83, 47.80, 56.68]],
            [200000, [41.59, 51.78, 51.01]]
        ]
    }
]

var bench_var_query_size = [
    {
        "label": "From 200k series",
        "data": [
            [5, [20.25, 18.40, 18.73]],
            [10, [32.37, 36.16, 39.31]],
            [20, [49.38, 61.06, 51.06]],
            [40, [73.80, 65.73, 68.95]],
            [80, [112.14, 112.76, 114.44]],
            [160, [212.45, 193.70, 209.62]],
            [240, [322.37, 346.85, 348.83]],
            [320, [565.211, 573.63, 488.67]],
            [400, [561.83, 612.19, 679.79]]
        ]
    }
]


var line_plot = function(data, container, xtext) {
    var margin = {top: 20, right: 40, bottom: 20, left: 40},
        width = $(container).width() - margin.right - margin.left,
        height = 500 - margin.top - margin.bottom;

    var x = d3.scale.linear().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);

    var xAxis = d3.svg.axis().scale(x).orient("bottom"),
        yAxis = d3.svg.axis().scale(y).orient("left");

    var line = d3.svg.line()
               .x(function(d){return x(d[0]);})
               .y(function(d){return y(d[1]);});

    var svg = d3.select(container).append("svg")
              .attr("width", width+margin.right+margin.left)
              .attr("height", height+margin.top+margin.bottom)
              .append("g")
              .attr("transform", "translate("+margin.left+","+margin.top+")");

    x.domain(d3.extent(data, function(d){return d[0];})).nice();
    y.domain(d3.extent(data, function(d){return d[1];})).nice();

    svg.append("g").attr("class", "x axis")
    .attr("transform", "translate(0,"+height+")").call(xAxis)
    .append("text").attr("x", width).attr("y", -20).attr("dy", "1em")
    .style("text-anchor", "end").text(xtext);

    svg.append("g").attr("class", "y axis").call(yAxis).append("text")
    .attr("transform", "rotate(-90)").attr("y", 6).attr("dy", "1em")
    .style("text-anchor", "end").text("Import Time (s)");

    svg.append("path").datum(data).attr("class", "line").attr("d", line)
           .attr("stroke", "steelblue");
};

var scatter_plot = function(data, container, xtext) {
    var margin = {top: 20, right: 40, bottom: 20, left: 40},
        width = $(container).width() - margin.right - margin.left,
        height = 500 - margin.top - margin.bottom;

    var x = d3.scale.linear().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);

    var color = d3.scale.category10();
    var xAxis = d3.svg.axis().scale(x).orient("bottom"),
        yAxis = d3.svg.axis().scale(y).orient("left");

    var svg = d3.select(container).append("svg")
              .attr("width", width+margin.right+margin.left)
              .attr("height", height+margin.top+margin.bottom)
              .append("g")
              .attr("transform", "translate("+margin.left+","+margin.top+")");

    var scatter_data = [];
    data.forEach(function(d){
        d.data.forEach(function(l){
            l[1].forEach(function(e){
                scatter_data.push({
                    label: d.label,
                    x: l[0],
                    y: e
                });
            })
        });
    });

    var line_data = data.map(function(d){
        d.data.forEach(function(l){
            l[1] = l[1].map(function(x, i, arr){return x/arr.length;})
                       .reduce(function(a, b){return a+b;});
        });
        return d;
    });

    var line = d3.svg.line()
               .x(function(d){return x(d[0]);})
               .y(function(d){return y(d[1]);});

    x.domain(d3.extent(scatter_data, function(d){return d.x;})).nice();
    y.domain(d3.extent(scatter_data, function(d){return d.y;})).nice();

    svg.append("g").attr("class", "x axis")
    .attr("transform", "translate(0,"+height+")").call(xAxis)
    .append("text").attr("x", width).attr("y", -20).attr("dy", "1em")
    .style("text-anchor", "end").text(xtext);

    svg.append("g").attr("class", "y axis").call(yAxis).append("text")
    .attr("transform", "rotate(-90)").attr("y", 6).attr("dy", "1em")
    .style("text-anchor", "end").text("Query Time (ms)");

    line_data.forEach(function(ld){
        svg.append("path").datum(ld.data)
           .attr("stroke", color(ld.label))
           .attr("class", "line").attr("d", line);
    });

    svg.selectAll(".dot").data(scatter_data).enter().append("circle")
       .attr("class", "dot").attr("r", 3.5)
       .attr("cx", function(d){return x(d.x);})
       .attr("cy", function(d){return y(d.y);})
       .attr("fill", function(d){return color(d.label);});


    var legend = svg.selectAll(".legend").data(color.domain()).enter()
                .append("g").attr("class", "legend")
                .attr("transform", function(d, i){return "translate(0, " + i * 20 + ")";});

    legend.append("rect").attr("x", width - 18).attr("width", 18).attr("height", 18).style("fill", color);

    legend.append("text").attr("x", width - 24).attr("y", 9).attr("dy", ".35em").style("text-anchor", "end").text(function(d){return d;});
}


$.get('/template/benchmark.mustache', function(template){
    var rendered = Mustache.render(template, {});
    $('#main').html(rendered);
    line_plot(data_bench_import_fix_frag, "#import-1", "Series Size");
    line_plot(data_bench_var_frag, "#import-2", "Fragment Level");
    scatter_plot(bench_var_coll_size, "#query-1", "Series Size");
    scatter_plot(bench_var_query_size, "#query-2", "Query Size(d)");
});

var api_root = "http://172.18.57.100:12345";
// var api_root = "http://localhost:12345";
function angle(d) {
      var a = (d.startAngle + d.endAngle) * 90 / Math.PI - 90;
      return a > 90 ? a - 180 : a;
}

$(document).ready(function(){
    var infoFetched = false;
    var nekosInfo = {};

    var fetchInfo = function(callback) {
        var getSeries = $.getJSON(api_root+"/peers/", function(r){
            $.extend(nekosInfo, r);
        });
        var getPeers = $.getJSON(api_root+"/series/", function(r){
            $.extend(nekosInfo, r);
        });
        $.when(getSeries, getPeers).then(function() {
            infoFetched = true;
            if (callback != undefined)
                callback();
        });
    };


    var handle_dashboard = function(){
        $.get('/template/dashboard.mustache', function(template) {
            fetchInfo(function(){
                var rendered = Mustache.render(template, nekosInfo);
                $('#main').html(rendered);
                var colors = {
                    "arch-nekod-1": "SkyBlue",
                    "arch-nekod-2": "LightGreen",
                    "arch-nekod-3": "Gold",
                    "arch-nekod-4": "Tomato"
                }
                var drawHashRing = function() {
                    var width = $('#hash-ring-container').width(),
                        height = 500,
                        radius = Math.min(width, height)/2;

                    var arc = d3.svg.arc()
                        .outerRadius(radius - 10)
                        .innerRadius(radius - 110);

                    var pie = d3.layout.pie()
                        .sort(null)
                        .value(function(d) { return d.h; });

                    var svg = d3.select("#hash-ring-container").append("svg")
                        .attr("width", width)
                        .attr("height", height)
                        .append("g")
                        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

                    var pie_data = nekosInfo.peers.map(function(e){
                        return {name: e.real_name, h: e.hash_value};
                    });

                    pie_data[0].h = (Math.pow(2, 32) - 1 - pie_data[pie_data.length-1].h) + pie_data[0].h;
                    for (var i = 1; i < pie_data.length; i++) {
                        pie_data[i].h = pie_data[i].h - pie_data[i-1].h;
                    }

                    var g = svg.selectAll(".arc")
                            .data(pie(pie_data))
                            .enter().append("g")
                            .attr("class", "arc");

                    g.append("path")
                        .attr("d", arc)
                        .style("fill", function(d) { return colors[d.data.name]; });


                    g.append("text")
                        .attr("transform", function(d) {
                            return "translate(" + arc.centroid(d) + ")rotate("+angle(d)+")";
                        })
                        .attr("dy", ".5em")
                        .style("text-anchor", "middle")
                        .style("font", "bold 12px Monospace")
                        .text(function(d) { return d.data.name; });
                };

                var drawCountRing = function(seriesName) {
                    var width = $('#series-count-'+seriesName).width(),
                        height = Math.floor(width*0.8),
                        radius = Math.floor(height/3);

                    var arc = d3.svg.arc()
                        .outerRadius(radius)
                        .innerRadius(radius*0.4);

                    var pie = d3.layout.pie()
                        .sort(function(a, b) {return a.count > b.count;})
                        .value(function(d) { return d.count; });

                    var svg = d3.select("#series-count-"+seriesName).append("svg")
                        .attr("width", width)
                        .attr("height", height)
                        .append("g")
                        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

                    d3.json(api_root+"/series/"+seriesName+"/meta", function(error, j){
                        var data = j.backends;

                        var g = svg.selectAll(".arc")
                                .data(pie(data))
                                .enter().append("g")
                                .attr("class", "arc");

                        g.append("path")
                            .attr("d", arc)
                            .style("fill", function(d) { return colors[d.data.name]; });


                        g.append("text")
                            .attr("transform", function(d) {
                                var c = arc.centroid(d),
                                x = c[0],
                                y = c[1],
                                // pythagorean theorem for hypotenuse
                                h = Math.sqrt(x*x + y*y);
                                return "translate(" + (x/h * radius/2) +  ',' +
                                    (y/h * radius/2) +  ")";
                            })
                            .attr("dy", ".5em")
                            .style("text-anchor", function(d){
                                return (d.endAngle + d.startAngle)/2 > Math.PI ? "end" : "start";
                            })
                            .text(function(d) { return d.data.name; });
                    });

                }
                drawHashRing();
                nekosInfo.series.forEach(function(s){
                    drawCountRing(s.name);
                });
            });
        })
    };
    var handle_plot = function(){
        $.get('/template/plot.mustache', function(template){
            var rendered = Mustache.render(template, nekosInfo);
            $('#main').html(rendered);
            $("#series-select").html();
            for(var i=0; i < nekosInfo.series.length; i++) {
                $('#series-select').append("<option>"+nekosInfo.series[i].name+"</option>");
            }

            $('.date').datepicker({todayHighlight: true});
            var options = {
                series: {
                    lines: {show: true}
                },
                legend: {show: true },
                xaxis: {mode: "time"}
            };
            var plot = $.plot("#canvas", [{data: []}], options);

            $('.series-option').click(function(){
                if ($(this).hasClass('selected')) {
                    $(this).removeClass('selected').removeClass('label-success').addClass('label-default');
                } else {
                    $(this).addClass('selected').addClass('label-success').removeClass('label-default');
                }
            });

            $("#draw").click(function(){
                var plot_data = [];
                var series_list = [],
                    start = $("#date-start").val(),
                    end = $("#date-end").val();
                $("#series-selectors .series-option.selected").each(function(i, e){
                    series_list.push($(e).text());
                });

                var defered = series_list.map(function(e){
                    var url = api_root + "/series/" + e + "?" + $.param({start: start, end: end});
                    return $.getJSON(url, function(r){
                        plot_data.push(r);
                        plot_data.sort(function(a, b){
                            if (a.label > b.label)
                                return 1;
                            if (b.label > a.label)
                                return -1;
                            return 0;
                        });
                        console.log(r.label);
                    });
                });

                $.when.apply($, defered).then(function(){
                    plot.setData(plot_data);
                    plot.setupGrid();
                    plot.draw();
                });
            });

        });
    };

    var routes = {
        '/': handle_dashboard,
        '/dashboard': handle_dashboard,
        '/plot': function(){
            if(!infoFetched) {
                fetchInfo(handle_plot);
            } else {
                handle_plot();
            }
        }
    };

    var router = Router(routes);
    router.init();

    if (window.location.hash == "") {
        window.location.hash = "/"
    }

});

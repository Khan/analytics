var w = 1000,
    h = 800,
    fill = d3.scale.category20(),
    linkDistance=800,
    timeout = null,
    node = null,
    path = null,
    oldMatches = null;


d3.json('/static/cover_graph.json', function(json) {
  var vis = d3.select("#chart")
      .append("svg:svg")
        .attr("width", "100%")
        .attr("height", "100%")
        .on("click", function() {if(node && path) clear(1);})
      .append('svg:g')
        .call(d3.behavior.zoom().scaleExtent([0.25,8]).on("zoom", redraw))
      .append('svg:g');

  vis.append('svg:rect')
    .attr("width", w)
    .attr("height", h)
    .attr("fill", "white")
    .style("pointer-events", 'all')

  vis.append("svg:defs").selectAll("marker")
      .data(["end"])
    .enter().append("svg:marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 20)
      .attr("refY", -2)
      .attr("markerWidth", 9)
      .attr("markerHeight", 9)
      .attr("orient", "auto")
    .append("svg:path")
      .attr("d", "M0,-5L10,0L0,5");

  function redraw() {
    vis.attr("transform",
        "translate(" + d3.event.translate + ")"
        + " scale(" + d3.event.scale + ")");
  }

  var force = d3.layout.force()
      .nodes(json.nodes)
      .links(json.links)
      .size([w,h])
      .charge([-2000])
      .gravity(0.5)
      .on("tick", tick)
      .start();

  // Adjacency matrix
  var adjMatrix = {};
  json.links.forEach(function (d) {
    adjMatrix[d.source.index + "," + d.target.index] = 1
  });

  // returns list of id's of nodes covering v1
  function getCovers(v1) {
    covers = new Array();
    json.nodes.forEach(function (v2) {
      if(adjMatrix[v2.index + "," + v1.index])
        covers.push(v2);
    })
    return covers;
  }

  // returns list of id's of nodes covered by v1
  function getCovered(v1) {
    covered = new Array();
    json.nodes.forEach(function (v2) {
      if(adjMatrix[v1.index + "," + v2.index])
        covered.push(v2);
    })
    return covered;
  }

  // check if node is connected to any node in arr
  function connected(v1, arr) {
    return arr.some( function(v2) {
      return adjMatrix[v1.index + "," + v2.index] || adjMatrix[v2.index + "," + v1.index] || v1.index == v2.index;
    });
  }

  // check if edge is adjacent to any node in arr
  function adjacent(e, arr) {
    return arr.some( function(v) {
      return e.source == v || e.target == v;
    });
  }

  var tooltip = d3.select("body").append("div")   
    .attr("class", "tooltip")               
    .style("opacity", 0)

  var path = vis.selectAll("path.link")
    .data(json.links)
  .enter().append("svg:path")
    .attr("class", "link")
    .attr("marker-end", "url(#end)")
    .attr("data", function(d) { return "source:"+" target:"+d.target })

  var node = vis.selectAll("circle.node")
    .data(json.nodes)
  .enter().append("svg:circle")
    .attr("class", "node")
    .attr("id", function(d) { return d.id; })
    .attr("cx", function(d) { return d.x; })
    .attr("cy", function(d) { return d.y; })
    .attr("r", 15)
    .style("fill", function(d) { return fill(d.id); })
    .on("mouseover", function(d) {
      tooltip
        .style("opacity", 1)
        .text(d.id)
      val = $('#divs').val();
      if(val == "") {
        d3.select(this)
          .transition()
          .duration(400)
          .attr("r", 30)
      }
    })
    .on("mousemove", function(){
      return tooltip.style("top", (event.pageY-10)+"px").style("left",(event.pageX+10)+"px");
    })

    .on("click", function(d) {
      specialZoom(d, 3);
    })
    .on("mouseleave", function(d) { 
      val = $('#divs').val();
      if(val == "")
        clear(1);
    })
    .on("mouseout", function(){return tooltip.style("opacity", 0);})
    .call(force.drag);

  vis.style("opacity", 1e-6)
  .transition()
    .duration(1000)
    .style("opacity", 1);

  node.append("text")
      .attr("x", 12)
      .attr("dy", ".35em")
      .text(function(d) { return d.source; });


  function tick() {
    path.attr("d", function(d) {
      var dx = d.target.x - d.source.x,
          dy = d.target.y - d.source.y,
          dr = Math.sqrt(dx * dx + dy * dy);
      return "M" + 
          d.source.x + "," + 
          d.source.y + "A" + 
          dr + "," + dr + " 0 0,1 " + 
          d.target.x + "," + 
          d.target.y;
    });

    node.attr("cx", function(d) { return d.x; })
        .attr("cy", function(d) { return d.y; });
  }

  function clear(opacity) {
    node
      .transition()
      .duration(200)
      .style("opacity", opacity)
      .attr("r", 15)
    path
      .transition()
      .duration(200)
      .style("opacity", opacity)
  }

  var zoom = d3.behavior.zoom();

  function specialZoom(d, factor) {
    x = vis.selectAll("#" + d.id);
    var transx = (-parseInt(x.attr("cx")) * factor + w / 2),
        transy = (-parseInt(x.attr("cy")) * factor + h / 2);
    vis.transition()
      .attr("transform", "translate(" + transx + "," + transy + ")scale(" + factor + ")");
    zoom.scale(factor);
    zoom.translate([transx, transy]);
  }

  function show(matches, background, foreground) {
    
    matchesArray = new Array();
    matches
      .each(function(d) {
        matchesArray.push(d);
      });

    if(oldMatches){
      oldMatches.filter(function(d) {
          return jQuery.inArray(d, matchesArray) == -1;
      }).attr("r", 15)
      matches.filter(function(d) {
          return jQuery.inArray(d, oldMatches) == -1;
      }).attr("r", 30)
    }
    else
      matches.attr("r", 30)         
    oldMatches = matches;

    node
      .each(function(d) {
          opacity = connected(d, matchesArray) ? foreground : background;
          d3.select(this)
            .transition()
            .duration(200)
            .style("opacity", opacity)
      })
    path
      .each(function(d) {
          opacity = adjacent(d, matchesArray) ? foreground : background;
          d3.select(this)
            .transition()
            .duration(200)
            .style("opacity", opacity)
      })

    matchesArray.forEach(function (d) {
      infoString = '<div class="card"><div class="card-title" id="card-"' + d.id + '"><h5>' + d.id + '</h5></div>'
      var covers = getCovers(d);
      var covered = getCovered(d);
      if(covers.length != 0){
        infoString += '<div class="cover-list">Covered by: '
        covers.forEach(function (d) {
          infoString += d.id + ', ';
        })
        infoString += '</div>';
      }
      if(covered.length != 0){
        infoString += '<div class="cover-list">Covers: '
        covered.forEach(function (d) {
          infoString += d.id + ', ';
        })
        infoString += '</div>';
      }
      infoString += '</div>';      
      $('#ex-info').append(infoString);
      $('.card-title').data("node", d)
    })
  }

  $('#divs').keyup(function(event) {
    $('#ex-info').empty();
    val = $('#divs').val();
    if(val != ""){
      matches = node
        .filter(function (d) {
          return ((d.id).indexOf(val) == 0);
      })
      show(matches, 0.1, 1);
    }
    else
      clear(1);
  });

  $('.card-title').on("click", function() {
    alert("asdf");
  })

});

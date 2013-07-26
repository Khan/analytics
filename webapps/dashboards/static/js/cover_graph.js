var w = 1000,
    h = 800,
    fill = d3.scale.category20(),
    linkDistance=800,
    timeout = null,
    node = null,
    path = null,
    oldMatches = null,
    nodePadding = 10;

d3.json('/static/cover_graph.json', function(json) {

  json.nodes.forEach(function (d) {
    d.edges = [];
  })
  json.links.forEach(function (e) {
    json.nodes[e.source].edges.push(e);
    json.nodes[e.target].edges.push(e);
  });
  json.links.forEach(function (e) {
    e.source = json.nodes[e.source];
    e.target = json.nodes[e.target];
  })

  // Adjacency matrix
  var adjMatrix = {};
  json.links.forEach(function (d) {
    adjMatrix[d.source.id + "," + d.target.id] = 1
  });

  // helper function for checking whether v1 -> v2 is an edge
  function isEdge(v1, v2) {
    if(adjMatrix[v1.id + "," + v2.id])
      return true;
    else
      return false;
  }

  // returns list of id's of nodes covering v1
  function getCovers(v1) {
    var covers = new Array();
    json.nodes.forEach(function (v2) {
      if(isEdge(v2,v1))
        covers.push(v2);
    })
    return covers;
  }

  // returns list of id's of nodes covered by v1
  function getCovered(v1) {
    var covered = new Array();
    json.nodes.forEach(function (v2) {
      if(isEdge(v1,v2))
        covered.push(v2);
    })
    return covered;
  }

  // check if node is connected to any node in arr
  function connected(v1, arr) {
    return arr.some( function(v2) {
      return (isEdge(v1,v2)|| isEdge(v2,v1) || v1.id == v2.id);
    });
  }

  // check if edge is adjacent to any node in arr
  function adjacent(e, arr) {
    return arr.some( function(v) {
      return e.source == v || e.target == v;
    });
  }

  function getAdjacent(v) {
    var adjacentEdges = new Array();
    json.links.forEach(function (e) {
      if(adjMatrix[v.id + ',' + e.source.id] || adjMatrix[v.id + ',' + e.source.id])
        adjacentEdges.push(e);
    })
    return adjacentEdges;
  }

  function getChildren(v) {
    var children = [];
    var edges = []
    function getChildrenHelper(v1) {
      children.push(v1)
      json.nodes.forEach(function (v2) {
        if(isEdge(v1, v2)){
          var result = json.links.filter(function (d) {
            return (d.source.id == v1.id && d.target.id == v2.id);
          })[0];
          edges.push(result);
          getChildrenHelper(v2);
        }
      })
    }
    getChildrenHelper(v);
    return [children, edges];
  }

  function getParents(v) {
    var parents = [];
    var edges = []
    function getParentsHelper(v1) {
      parents.push(v1)
      json.nodes.forEach(function (v2) {
        if(isEdge(v2, v1)){
          var result = json.links.filter(function (d) {
            return (d.source.id == v2.id && d.target.id == v1.id);
          })[0];
          edges.push(result);
          getParentsHelper(v2);
        }
      })
    }
    getParentsHelper(v);
    return [parents, edges];
  }

  function getConnectedComponent(v) {
    nodes = getChildren(v)[0].concat(getParents(v)[0]);
    edges = getChildren(v)[1].concat(getParents(v)[1]);
    return [nodes, edges];
  }

  displayConnectedComponent(json.nodes, json.links)

  $('#divs').keyup(function(event) {
    $('#ex-info').empty();
    val = $('#divs').val();
    if(val != ""){
      matches = json.nodes
        .filter(function (d) {
          return ((d.id).indexOf(val) == 0);
      })
      var tmp_nodes = [];
      var tmp_links = [];

      matches.forEach(function (d) {
        tmp_nodes = tmp_nodes.concat(getConnectedComponent(d)[0]);
        tmp_links = tmp_links.concat(getConnectedComponent(d)[1]);
      })

      var nodes = []
      nodes = tmp_nodes.filter(function (d, i) {
        return tmp_nodes.indexOf(d) == i;
      })
      tmp_links = tmp_links.filter(function (d, i) {
        return tmp_links.indexOf(d) == i;
      })

      var results = displayConnectedComponent(nodes, tmp_links)

      // matches.forEach(function (d) {
      //   infoString = '<div class="card"><div class="card-title" id="card-"' + d.id + '">' +d.id + '</div>'
      //   var covers = getCovers(d);
      //   var covered = getCovered(d);
      //   if(covers.length != 0){
      //     infoString += '<div class="cover-list">Covered by: '
      //     var max = covers.length;
      //     var counter = 0;
      //     covers.forEach(function (d) {
      //       counter += 1;
      //       infoString += d.id;
      //       if(counter != max)
      //         infoString += ', ';
      //     })
      //     infoString += '</div>';
      //   }
      //   if(covered.length != 0){
      //     infoString += '<div class="cover-list">Covers: '
      //     covered.forEach(function (d) {
      //       infoString += d.id + ', ';
      //     })
      //     infoString += '</div>';
      //   }
      //   infoString += '</div>';      
      //   $('#ex-info').append(infoString);
      //   $('.card-title').data("node", d)
      // })
      
      path = results[0];
      node = results[1];

      console.log(node)
      node
        .each(function(d) {
            opacity = connected(d, matches) ? 1 : 0.3;
            stroke = (matches.indexOf(d) > -1) ? 5 : 1;
            d3.select(this)
              .style("opacity", opacity)
              .style("stroke-width", stroke)
        })
      path
        .each(function(d) {
            opacity = adjacent(d, matches) ? 1 : 0.3;
            d3.select(this)
              .style("opacity", opacity)
        })
    }
    else
      displayConnectedComponent(json.nodes, json.links)
  });

});


function displayConnectedComponent(nodes, links) {
  function spline(e) {
    var points = e.dagre.points.slice(0);
    var source = dagre.util.intersectRect(e.source.dagre, points[0]);
    var target = dagre.util.intersectRect(e.target.dagre, points[points.length - 1]);
    points.unshift(source);
    points.push(target);
    return d3.svg.line()
      .x(function(d) { return d.x; })
      .y(function(d) { return d.y; })
      .interpolate("linear")
      (points);
  }

  // Translates all points in the edge using `dx` and `dy`.
  function translateEdge(e, dx, dy) {
    e.dagre.points.forEach(function(p) {
      p.x = Math.max(0, Math.min(svgBBox.width, p.x + dx));
      p.y = Math.max(0, Math.min(svgBBox.height, p.y + dy));
    });
  }

  function redraw() {
    vis.attr("transform",
        "translate(" + d3.event.translate + ")"
        + " scale(" + d3.event.scale + ")");
  }

  // empty the canvas
  $("#chart").empty()

  var vis = d3.select("#chart")
      .append("svg:svg")
        .on("click", function() {if(node && path) clear(1);})
        .call(d3.behavior.zoom().on("zoom", redraw))
      .append('svg:g')

  vis.append("svg:defs").selectAll("marker")
      .data(["end"])
    .enter().append("svg:marker")
      .attr("id", "end")
      .attr("viewBox", "0 0 10 10")
      .attr("refX", 8)
      .attr("refY", 5)
      .attr("markerWidth", 8)
      .attr("markerHeight", 5)
      .attr("orient", "auto")
    .append("svg:path")
      .attr("d", "M 0 0 L 10 5 L 0 10 z");

  var path = vis.selectAll("path.link")
    .data(links)
  .enter().append("svg:path")
    .attr("class", "link")
    .attr("marker-end", "url(#end)")

  var node = vis.selectAll("g.node")
    .data(nodes)
    .enter()
      .append("g")
      .attr("class", "node")
      .attr("id", function(d) { return d.id; })

    // .on("mouseover", function(d) {
    //   tooltip
    //     .style("opacity", 1)
    //     .text(d.id)
    //   val = $('#divs').val();
    //   if(val == "") {
    //     d3.select(this)
    //       .transition()
    //       .duration(400)
    //       .attr("r", 30)
    //   }
    // })
    .on("mouseleave", function(d) { 
      val = $('#divs').val();
      if(val == "")
        clear(1);
    })
    // .on("mouseout", function(){return tooltip.style("opacity", 0);})

  var rects = node
    .append("rect")

  var labels = node
    .append("text")
      .attr("text-anchor", "middle")
      .attr("x", 0);

  labels
    .append("tspan")
    .attr("x", 0)
    .attr("dy", "1em")
    .text(function(d) { return d.id; });

  labels.each(function(d) {
    var bbox = this.getBBox();
    d.bbox = bbox;
    d.width = bbox.width + 2 * nodePadding;
    d.height = bbox.height + 2 * nodePadding;
  });

  rects
    .attr("x", function(d) { return -(d.bbox.width / 2 + nodePadding); })
    .attr("y", function(d) { return -(d.bbox.height / 2 + nodePadding); })
    .attr("width", function(d) { return d.width; })
    .attr("height", function(d) { return d.height; })

  labels
    .attr("x", function(d) { return -d.bbox.width / 2; })
    .attr("y", function(d) { return -d.bbox.height / 2; });

  dagre.layout()
    .nodeSep(50)
    .edgeSep(10)
    .rankSep(50)
    .nodes(nodes)
    .edges(links)
    .debugLevel(1)
    .run();

  node.attr("transform", function(d) { return 'translate('+ d.dagre.x +','+ d.dagre.y +')'; });

  path.each(function(d) {
    var points = d.dagre.points;
    if (!points.length) {
      var s = d.source.dagre;
      var t = d.target.dagre;
      points.push({ x: (s.x + t.x) / 2, y: (s.y + t.y) / 2 });
    }

    if (points.length === 1) {
      points.push({ x: points[0].x, y: points[0].y });
    }
  });

  path
    // Set the id. of the SVG element to have access to it later
    .attr('id', function(e) { return e.dagre.id; })
    .attr("d", function(e) { return spline(e); });

  var svgBBox = vis.node().getBBox();
  vis.attr("width", svgBBox.width + 10);
  vis.attr("height", svgBBox.height + 10);


  var nodeDrag = d3.behavior.drag()
    // Set the right origin (based on the Dagre layout or the current position)
    .origin(function(d) { return d.pos ? {x: d.pos.x, y: d.pos.y} : {x: d.dagre.x, y: d.dagre.y}; })
    .on('drag', function (d, i) {
      var prevX = d.dagre.x,
          prevY = d.dagre.y;

      // The node must be inside the SVG area
      d.dagre.x = Math.max(d.width / 2, Math.min(svgBBox.width - d.width / 2, d3.event.x));
      d.dagre.y = Math.max(d.height / 2, Math.min(svgBBox.height - d.height / 2, d3.event.y));
      d3.select(this).attr('transform', 'translate('+ d.dagre.x +','+ d.dagre.y +')');

      var dx = d.dagre.x - prevX,
          dy = d.dagre.y - prevY;

      // Edges position (inside SVG area)
      d.edges.forEach(function(e) {
        translateEdge(e, dx, dy);
        d3.select('#'+ e.dagre.id).attr('d', spline(e));
      });
    });

  var edgeDrag = d3.behavior.drag()
    .on('drag', function (d, i) {
      translateEdge(d, d3.event.dx, d3.event.dy);
      d3.select(this).attr('d', spline(d));
    });

  node.call(nodeDrag);
  path.call(edgeDrag);

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
    path
      .transition()
      .duration(200)
      .style("opacity", opacity)
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
      }).attr("height", 30)
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
      infoString = '<div class="card"><div class="card-title" id="card-"' + d.id + '">' +d.id + '</div>'
      var covers = getCovers(d);
      var covered = getCovered(d);
      if(covers.length != 0){
        infoString += '<div class="cover-list">Covered by: '
        var max = covers.length;
        var counter = 0;
        covers.forEach(function (d) {
          counter += 1;
          infoString += d.id;
          if(counter != max)
            infoString += ', ';
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
  return [path, node]
}


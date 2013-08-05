var fill = d3.scale.category20(),
    nodePadding = 10,
    // each node is an object with an id (the name of the exercise)
    // as well as a list of links of the form {source: <sourceID>, 
    // target: <targetID>}. each link in graph is of the form
    // {source: <sourceObject>, target: <targetObject>}
    graph = {nodes: [], links: []};

$.get("http://www.khanacademy.org/api/v1/topictree", function(data) {
    // helper function to get the index of a node in the list of nodes arr,
    // (which will be graph.nodes) by id
    function indexOfNode(arr, nodeID) {
        return arr.map(function(e) { return e.id; }).indexOf(nodeID);
    }

    // helper function to determine whether edge from source to target
    // exists in the graph arr, which will be graph.nodes.\
    function edgeExists(arr, source, target) {
        return arr.some(function(e) { 
            return e.source === source && e.target === target; })
    }

    // parse data to construct rudimentary version of graph
    function makeGraph(top) {
        if(top.covers) {
            for(i in top.covers) {
                var source = top.node_slug.substring(2)
                var target = top.covers[i]
                var newLinkDict = {source: source, target: target};
                var sourceIndex = indexOfNode(graph.nodes, source);
                var targetIndex = indexOfNode(graph.nodes, target);
                if(!edgeExists(graph.links, source, target)) {
                    graph.links.push(newLinkDict);
                    if(sourceIndex > -1)
                        graph.nodes[sourceIndex].edges.push(newLinkDict);
                    else
                        graph.nodes.push({id: source, edges: [newLinkDict]});
                    if(targetIndex > -1)
                        graph.nodes[targetIndex].edges.push(newLinkDict);
                    else
                        graph.nodes.push({id: target, edges: [newLinkDict]});
                }
                
            }
        }
        if(!(top.children) || top.children.length < 1) {
            return;
        }
        for(i in top.children) {
            makeGraph(top.children[i]);
        }
    }

    makeGraph(data);

    // get the list of node IDs and construct out of this a dictionary
    // where (key, value) = (node ID, index in list), for easy access
    // to graph.nodes in the following chunk of code
    var nodeIDs = graph.nodes.map(function(e) { return e.id; });
    var IDmap = {};
    for(i in nodeIDs) {
        IDmap[nodeIDs[i]] = i;
    }

    // change the source and target of each link to be node objects,
    // not just exercise names
    graph.links.forEach(function (e) {
        e.source = graph.nodes[IDmap[e.source]];
        e.target = graph.nodes[IDmap[e.target]];
    });

    // Adjacency list
    var adjMatrix = {};
    graph.links.forEach(function (d) {
        adjMatrix[d.source.id + "," + d.target.id] = 1
    });

    // helper function for checking whether v1 -> v2 is an edge
    function isEdge(v1, v2) {
        if(adjMatrix[v1.id + "," + v2.id])
            return true;
        else
            return false;
    }

    // returns list of id"s of nodes covering v1
    function getCovers(v1) {
        var covers = new Array();
        graph.nodes.forEach(function (v2) {
            if(isEdge(v2,v1))
                covers.push(v2);
        })
        return covers;
    }

    // returns list of id"s of nodes covered by v1
    function getCovered(v1) {
        var covered = new Array();
        graph.nodes.forEach(function (v2) {
            if(isEdge(v1,v2))
                covered.push(v2);
        })
        return covered;
    }

    // check if node is connected to any node in arr
    function connected(v1, arr) {
        return arr.some( function(v2) {
            return (isEdge(v1,v2)|| isEdge(v2,v1) || v1.id === v2.id);
        });
    }

    // check if edge is adjacent to any node in arr
    function adjacent(e, arr) {
        return arr.some( function(v) {
            return e.source === v || e.target === v;
        });
    }

    // traverse by DFS and return children and edges traversed
    function getChildren(v) {
        var children = [];
        var edges = []
        function getChildrenHelper(v1) {
            children.push(v1)
            v1.edges.forEach(function(e) {
                if(v1.id === e.source.id) {
                    edges.push(e)
                    getChildrenHelper(e.target)
                }
            })
        }
        getChildrenHelper(v);
        return {children: children, edges: edges};
    }

    // traverse by DFS in reverse and return parents and edges traversed
    function getParents(v) {
        var parents = [];
        var edges = []
        function getParentsHelper(v1) {
            parents.push(v1)
            v1.edges.forEach(function(e) {
                if(v1.id === e.target.id) {
                    edges.push(e);
                    getParentsHelper(e.source);
                }
            })
        }
        getParentsHelper(v);
        return {parents: parents, edges: edges};
    }

    // get parents and children of node (not exactly the entire connected
    // component because it leaves out siblings of v)
    function getConnectedComponent(v) {
        parents = getParents(v).parents;
        children = getChildren(v).children;
        nodes = parents.concat(children);
        edges = getParents(v).edges.concat(getChildren(v).edges);
        parents.forEach(function(v) {
            parent.edges.forEach(function(e) {
                if(e.target in children)
                    edges.push(e);
            })            
        })
        return {nodes: nodes, edges: edges};
    }

    // Displays "connected components" (see above definition) of nodes matching
    // query, bolds borders of those nodes, fades out indirect covers/coverers
    function show(matches) {
        var tempNodes = [];
        var tempLinks = [];

        matches.forEach(function (d) {
            var connectedComponent = getConnectedComponent(d)
            tempNodes = tempNodes.concat(connectedComponent.nodes);
            tempLinks = tempLinks.concat(connectedComponent.edges);
        })

        var nodes = tempNodes.filter(function (d, i) {
            return tempNodes.indexOf(d) === i;
        })
        var links = tempLinks.filter(function (d, i) {
            return tempLinks.indexOf(d) === i;
        })

        var results = draw(nodes, links)

        path = results.path;
        node = results.node;
        rects = results.rects;

        opacities = {}
        strokes = {}
        node
            .each(function(d) {
                    opacities[d.id] = connected(d, matches) ? 1 : 0.4;
                    strokes[d.id] = (matches.indexOf(d) > -1) ? 5 : 0;
                    d3.select(this)
                        .style("opacity", opacities[d.id])
                        if(matches.indexOf(d) > -1)
                            d3.select(this)
                                .style("stroke", "black")
                                .style("text-stroke", 2)
                        .on("mouseover", function() {
                            d3.select(this)
                                .style("opacity", 0.2)
                        })
                        .on("mouseleave", function() {
                            d3.select(this)
                                .style("opacity", opacities[d.id])
                        })
            })
        path
            .each(function(d) {
                    opacity = adjacent(d, matches) ? 1 : 0.4;
                    d3.select(this)
                        .style("opacity", opacity)
            })
    }

    fade();
    draw(graph.nodes, graph.links);

    // when user starts searching, display matches using show(),
    // but if the input in the search bar is empty, show the entire graph
    // if there are no matches, show "no matches" in div id="warning"
    $(".search").keyup(function(event) {
        $(".warning").hide();
        val = $(".search").val();
        if(val != ""){
            var matches = graph.nodes
                .filter(function (d) {
                    return ((d.id).indexOf(val) === 0);
            })
            if(matches.length > 0) {
                show(matches);
            }
            else{
                $("#chart").empty();
                $(".warning").show();
            }
        }
        else {
            fade();
            draw(graph.nodes, graph.links);
        }
    });

    // we don"t want any ugly scroll bars
    $(window).resize(function() {
        $("svg").css("height", $(window).height() - 85)
    })

    // when home icon is pressed, return to initial display
    $(".home").on("click", function() {
        fade(); 
        draw(graph.nodes, graph.links); 
    });

    // fade into the entire covers graph to begin with (for the initial pageload and
    // for when the home button is pressed)
    function fade() {
        d3.select("#chart")
            .style("opacity", 1e-6)
            .transition()
                .duration(1000)
                .style("opacity", 1);  
    }

    // main function for rendering DAG out of list of nodes and links
    function draw(nodes, links) {
        // the next two cool functions are shamelessly borrowed from 
        // https://github.com/cpettitt/dagre for displaying edges fancily
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

        // get the height of the exercise in the tree to which it belongs, to be
        // used in coloring the blocks to indicate "importance" of an exercise
        function level(v) {
            var max = 0;
            for(i in links) {
                if(links[i].source.id === v.id) {
                    var maxNew = level(links[i].target);
                    if(max < maxNew) {
                        max = maxNew;
                    }
                }
            }
            return max + 1;
        }

        // empty the canvas
        $("#chart").empty()

        // create the canvas
        var vis = d3.select("#chart")
                .append("svg:svg")
                    .call(d3.behavior.zoom().on("zoom", redraw))
                .append("svg:g")

        // svg magic to create arrows for our edges, borrowed from
        // http://www.d3noob.org/2013/03/
        // d3js-force-directed-graph-example-basic.html
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

        // create all the edges
        var path = vis.selectAll("path.link")
            .data(links)
        .enter().append("svg:path")
            .attr("class", "link")
            .attr("marker-end", "url(#end)")

        // create all the nodes
        var node = vis.selectAll("g.node")
            .data(nodes)
            .enter()
                .append("g")
                .attr("class", "node")
                .attr("id", function(d) { return d.id; })
                .on("click", function(d) { show([d]); }) 

        // attach a rounded rectangle to each node, colored by level (see above)
        var rects = node
            .append("rect")
            .attr("rx", 10)
            .attr("ry", 10)
            .style("fill", function(d) { return fill(level(d)); })

        // attach a label to each node (code for positioning the text and 
        // rectangles again borrowed from https://github.com/cpettitt/dagre)
        var labels = node
            .append("text")
                .attr("text-anchor", "middle")
                .attr("x", 0)

        labels
            .append("tspan")
            .attr("x", 0)
            .attr("dy", "1em")
            .text(function(d) { return d.id; })
        
        labels
            .each(function(d) {
                var bbox = this.getBBox();
                d.bbox = bbox;
                d.width = bbox.width + 2 * nodePadding;
                d.height = bbox.height + 2 * nodePadding;
            });

        // (relatively) position and size the rectangles after adding labels
        rects
            .attr("x", function(d) { return -(d.bbox.width / 2 + nodePadding); })
            .attr("y", function(d) { return -(d.bbox.height / 2 + nodePadding); })
            .attr("width", function(d) { return d.width; })
            .attr("height", function(d) { return d.height; })

        // (relatively) position the labels inside the rectangles
        labels
            .attr("x", function(d) { return -d.bbox.width / 2; })
            .attr("y", function(d) { return -d.bbox.height / 2; });

        // render the DAG
        dagre.layout()
            .nodeSep(50)
            .edgeSep(10)
            .rankSep(50)
            .nodes(nodes)
            .edges(links)
            .debugLevel(1)
            .run();

        // absolutely position each node based on where the DAG 
        // rendered has placed them
        node.attr("transform", function(d) { return "translate("+ d.dagre.x +","+ d.dagre.y +")"; });

        // again, source: https://github.com/cpettitt/dagre
        // the next chunk of code gets a list of spline points for the edges,
        // makes each edge into a "bent" (splined) arrow, and adds dragging
        // functionality for all of the nodes and edges
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
            .attr("id", function(e) { return e.dagre.id; })
            .attr("d", function(e) { return spline(e); });

        var svgBBox = vis.node().getBBox();
        vis.attr("width", svgBBox.width + 10);
        vis.attr("height", svgBBox.height + 10);


        var nodeDrag = d3.behavior.drag()
            // Set the right origin (based on the Dagre layout or the current position)
            .origin(function(d) { return d.pos ? {x: d.pos.x, y: d.pos.y} : {x: d.dagre.x, y: d.dagre.y}; })
            .on("drag", function (d, i) {
                var prevX = d.dagre.x,
                        prevY = d.dagre.y;

                // The node must be inside the SVG area
                d.dagre.x = Math.max(d.width / 2, Math.min(svgBBox.width - d.width / 2, d3.event.x));
                d.dagre.y = Math.max(d.height / 2, Math.min(svgBBox.height - d.height / 2, d3.event.y));
                d3.select(this).attr("transform", "translate("+ d.dagre.x +","+ d.dagre.y +")");

                var dx = d.dagre.x - prevX,
                        dy = d.dagre.y - prevY;

                // Edges position (inside SVG area)
                d.edges.forEach(function(e) {
                    translateEdge(e, dx, dy);
                    d3.select("#"+ e.dagre.id).attr("d", spline(e));
                });
            });

        var edgeDrag = d3.behavior.drag()
            .on("drag", function (d, i) {
                translateEdge(d, d3.event.dx, d3.event.dy);
                d3.select(this).attr("d", spline(d));
            });

        node.call(nodeDrag);
        path.call(edgeDrag);

        // now that we have the entire graph drawn, make the graph wide enough to
        // fit into the window and change the height of the svg so that we have 
        // no scroll bars
        var w = d3.select("g").attr("width") * 1.005;
        var h = d3.select("g").attr("height") * 1.005;
        d3.select("svg")
            .attr("viewBox", "0 0 " + w + " " + h)    
            .attr("preserveAspectRatio", "XMinYMin meet")
            .style("height", $(window).height() - 85)
        // we return path and node so we can alter their styles during
        // search outside the scope of draw. we return rects to attach
        // click event listeners to each box
        return {path: path, node: node, rects: rects}
    }

});
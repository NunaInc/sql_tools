//
// nuna_sql_tools: Copyright 2022 Nuna Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
var beforeUnloadMessage = null;

var editor = ace.edit("editor");
editor.getSession().setMode("ace/mode/sql");

var resizeEvent = new Event("paneresize");
Split(['#editor', '#graph'], {
    sizes: [25, 75],
    onDragEnd: function() {
        var svgOutput =
            document.getElementById("svg_output");
        if (svgOutput != null) {
            svgOutput.dispatchEvent(resizeEvent);
        }
    }
});

var parser = new DOMParser();
var worker;

function sqlClicked(event, element) {
    // Id is formatted as <type>_<node_id>_<start_pos>_<end_pos>
    // With the positions as <line>_<column>. Please note that the line
    // is one based in python code, and zero based in the Ace editor.
    id_parts = element.id.split("_");
    if (id_parts.length != 4) {
        return;
    }
    start = id_parts[2].split("-");
    stop = id_parts[3].split("-");
    if (start.length != 2 || stop.length != 2) {
        return;
    }
    const range = new ace.Range(parseInt(start[0]) - 1, parseInt(start[1]),
                                parseInt(stop[0]) - 1, parseInt(stop[1]))
    editor.selection.setRange(range);
    editor.scrollToLine(parseInt(start[0]) - 1);
    if (event.shiftKey) {
        parseSQL(parseInt(id_parts[1]))
    }
}

function setError(error_message) {
    document.querySelector("#output").classList.add("error");
    var error = document.querySelector("#error");
    while (error.firstChild) {
        error.removeChild(error.firstChild);
    }
    error_message.split("\n").forEach(function(message) {
        error.appendChild(document.createTextNode(message));
        error.appendChild(document.createElement("br"));
    });
    console.error(error_message)
}

function clearError() {
    document.querySelector("#output").classList.remove("error");
}

var dot_content = "";
var last_sql_text = ""
var last_highlight_node = null;
const attr_names = [
    "rankdir", "newrank", "concentrate", "clusterrank", "ratio", "size"
];

function parseSQL(highlight_node) {
    const sql_text = editor.getSession().getDocument().getValue();
    updateSQL(sql_text, highlight_node);
}

function fillStockSQL(filename) {
    var xhttp = new XMLHttpRequest();
    xhttp.open("POST", "stocksql", true);
    xhttp.setRequestHeader('Content-Type', 'application/json');
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            editor.getSession().getDocument().setValue(this.responseText);
        }
    };
    xhttp.send(JSON.stringify({ sql: filename }));
}

function stockSQL() {
    const sql_text = document.querySelector("#stocksql select").value;
    if (sql_text != null && sql_text != "[None]") {
        fillStockSQL(sql_text);
        updateSQL(sql_text, null);
    }
}

function updateSQL(sql_text, highlight_node) {
    var xhttp = new XMLHttpRequest();
    xhttp.open("POST", "parsesql", true);
    xhttp.setRequestHeader('Content-Type', 'application/json');
    const svg_direct = document.getElementById("svg_direct").checked;
    const sql_highlight = document.getElementById("sql_highlight").checked;
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4) {
            if (this.status == 200) {
                clearError()
                if (svg_direct) {
                    updateOutput(this.responseText)
                } else {
                    updateGraph(this.responseText);
                }
            } else {
                setError(this.responseText);
            }
        }
    };
    var attrs = {};
    attr_names.forEach(function(name) {
        attrs[name] = document.querySelector("#" + name + " select").value;
    });
    var data = {
        sql: sql_text,
        detail: document.querySelector("#detail select").value,
        dialect: document.querySelector("#dialect select").value,
        attrs: attrs,
    };
    if (sql_highlight) {
        const selection = editor.getSelectionRange();
        data.selection_start_line = selection.start.row + 1;
        data.selection_start_column = selection.start.column;
        data.selection_stop_line = selection.end.row + 1;
        data.selection_stop_column = selection.end.column;
    }
    if (highlight_node) {
        data.highlight_node = highlight_node;
    }
    if (svg_direct) {
        data.generate_svg = 'true';
    }
    last_sql_text = sql_text;
    last_highlight_node = highlight_node;
    xhttp.send(JSON.stringify(data));
}

function updateDot() {
    if (!!last_sql_text) {
        updateSQL(last_sql_text, last_highlight_node);
    } else {
        parseSQL(null);
    }
}

function fillStockSQLs() {
    var xhttp = new XMLHttpRequest();
    xhttp.open("GET", "stocksql", true);
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            select = document.querySelector("#stocksql select")
            this.responseText.split("\n").forEach(function(fname, index) {
                opt = document.createElement('option');
                opt.innerHTML = fname;
                select.appendChild(opt);
            })
        }
    }
    xhttp.send(null);
}

function updateGraph(dot_content) {
    if (worker) {
        worker.terminate();
    }
    document.querySelector("#output").classList.add("working");
    document.querySelector("#output").classList.remove("error");
    worker = new Worker("./js/viz/worker.js");
    worker.onmessage = function(e) {
        document.querySelector("#output").classList.remove("working");
        clearError()
        updateOutput(e.data);
    }
    worker.onerror = function(e) {
        document.querySelector("#output").classList.remove("working");
        var message = e.message === undefined
            ? "An error occurred while processing the graph input."
            : e.message;
        setError(message);
        console.error(e);
        e.preventDefault();
    }
    var params = {
        src: dot_content,
    };
    worker.postMessage(params);
}

function updateOutput(result) {
    var graph = document.querySelector("#output");

    var svg = graph.querySelector("svg");
    if (svg) { graph.removeChild(svg); }

    var text = graph.querySelector("#text");
    if (text) { graph.removeChild(text); }

    var img = graph.querySelector("img");
    if (img) { graph.removeChild(img); }

    if (!result) { return; }

    var svg = parser.parseFromString(result, "image/svg+xml")
        .documentElement;
    svg.id = "svg_output";
    graph.appendChild(svg);

    svg_root = document.getElementById("graph0");
    if (svg_root) {
        Array.prototype.slice.call(svg_root.getElementsByTagName("g")).forEach(
            function(elem, index) {
                elem.setAttribute("onclick", "sqlClicked(event, this)");
            });
    }

    panZoom = svgPanZoom(
        svg, { zoomEnabled: true,
               controlIconsEnabled: true,
               fit: true,
               center: true,
               zoomScaleSensitivity: 0.4,
               minZoom: 0.1,
             });

    svg.addEventListener(
        'paneresize', function(e) { panZoom.resize(); }, false);
    window.addEventListener(
        'resize', function(e) { panZoom.resize(); });
}

window.addEventListener("beforeunload", function(e) {
    return beforeUnloadMessage;
});

document.querySelector("#detail select").addEventListener(
    "change", function() { updateDot(); });
attr_names.forEach(function(name) {
    document.querySelector("#" + name + " select").addEventListener(
        "change", function() { updateDot(); });
});

document.querySelector("#stocksql select").addEventListener(
    "change", function() { stockSQL(); });
document.querySelector("#svg_direct").addEventListener(
    "change", function() { updateDot(); });
document.querySelector("#sql_highlight").addEventListener(
    "change", function() { parseSQL(null); });

fillStockSQLs();

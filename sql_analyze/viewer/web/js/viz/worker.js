importScripts("./viz.js");

onmessage = function(e) {
    var viz = new Viz({workerURL: './full.render.js'})
    const promise = viz.renderString(e.data.src);
    promise.then(function(result) {
        postMessage(result);
    });
    promise.catch(function(error) {
        console.log(error);
    });
}

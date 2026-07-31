(function () {
    "use strict";

    var main = document.querySelector("main");
    var sidebarToggle = document.querySelector("#mdbook-sidebar-toggle");
    if (sidebarToggle) {
        sidebarToggle.setAttribute("role", "button");
    }

    if (!main || main.querySelector(".riffdb-status")) {
        return;
    }

    var status = document.createElement("aside");
    status.className = "riffdb-status";
    status.setAttribute("role", "note");
    status.textContent = "Proof of concept: local-only, pre-alpha compatibility. Review known limitations before production evaluation.";
    main.insertBefore(status, main.firstChild);
}());

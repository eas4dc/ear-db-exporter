// Mark the classes in the sidebar (bold, see custom.css), so they stand out from functions.
document.addEventListener("DOMContentLoaded", () => {
  for (const link of document.querySelectorAll(".wy-menu-vertical a[href*='#']")) {
    const target = document.getElementById(decodeURIComponent(link.hash.slice(1)));
    if (target && target.matches("dl.py.class > dt")) link.classList.add("toc-class");
  }
});

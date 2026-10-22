// Offline build of the utility classes used by server-rendered pages; rebuild with scripts/build-web-css.sh.
module.exports = {
  content: {
    relative: true,
    files: ["../templates/**/*.html", "../static/*.js", "../static/product/*.js"],
  },
  theme: { extend: {} },
  plugins: [],
};

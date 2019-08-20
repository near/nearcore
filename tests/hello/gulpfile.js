const gulp = require("gulp");
const nearUtils = require("near-shell/gulp-utils");

function build_wasm(done){
  nearUtils.compile("./assembly/main.ts", "./out/main.wasm", done);
};

const build = gulp.series(build_wasm);


exports.default = build;

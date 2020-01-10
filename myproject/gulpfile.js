const gulp = require("gulp");
const nearUtils = require("near-shell/gulp-utils");
const { exec } = require('child_process');

gulp.task("build", callback => {
  exec('mkdir -p out', ()=>{
    nearUtils.compile("./assembly/main.ts", "./out/main.wasm", callback);
  })
});

exports.default = gulp.series(["build"])

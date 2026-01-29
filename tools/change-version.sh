OLD="0.1.2"
NEW="0.1.3"

HERE=` basename "$PWD"`
if [[ "$HERE" != "tools" ]]; then
    echo "Please only execute in the tools/ directory";
    exit 1;
fi

# change version in all relevant files
find .. -name 'version.txt' -type f -exec perl -pi -e 's#'$OLD'#'$NEW'#' {} \;
find .. -name 'pom.xml' -type f -exec perl -pi -e 's#<version>'"$OLD"'</version>#<version>'"$NEW"'</version>#' {} \;
find .. -name 'README.md' -type f -exec perl -pi -e 's#<version>'"$OLD"'</version>#<version>'"$NEW"'</version>#' {} \;
find .. -name 'build.sbt' -type f -exec perl -pi -e 's#"clickhouse" % "'"$OLD"'"#"clickhouse" % "'"$NEW"'"#' {} \;

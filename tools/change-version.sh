NEW_VERSION=$(cat version.txt)

# change version in all relevant files
find .. -name 'pom.xml' -type f -exec perl -pi -e 's#<clickhouse.sink.version>[0-9\.]+</clickhouse.sink.version>#<clickhouse.sink.version>'"$NEW_VERSION"'</clickhouse.sink.version>#' {} \;
find .. -name 'README.md' -type f -exec perl -pi -e 's#<version>[0-9\.]+</version>#<version>'"$NEW_VERSION"'</version>#' {} \; # TODO: remove this as necessary
find .. -name 'build.sbt' -type f -exec perl -pi -e 's#val clickHouseVersion = "[0-9\.]+"#val clickHouseVersion = "'$NEW_VERSION'"#' {} \;

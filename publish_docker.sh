docker build --rm -t mtachon/geomcompare .;
docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD;
docker push mtachon/geomcompare;
docker logout;

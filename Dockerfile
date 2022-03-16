FROM python:3.9.7-slim-buster
RUN apt-get update && apt-get install -y \
  libsqlite3-mod-spatialite \
  libspatialite-dev \
  gdal-bin \
  python3-gdal \
  python-gdal \
  libgdal-dev \
  g++
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
RUN pip install --upgrade pip
RUN pip install ipython
RUN pip install numpy
RUN pip install setuptools
RUN pip install psycopg2
RUN pip install rtree
RUN pip install shapely
RUN pip install pyproj
RUN pip install GDAL==`gdal-config --version`
RUN pip install --upgrade geomcompare
WORKDIR /data
ENTRYPOINT ["ipython"]

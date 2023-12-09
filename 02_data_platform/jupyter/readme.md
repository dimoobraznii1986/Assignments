# Jupyter


## Docker

Create the image

```
docker build -t notebook . 
```

Run docker and open the port

```
docker run -p 8888:8888 notebook -d
```
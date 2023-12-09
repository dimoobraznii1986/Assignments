# Postgres DBT


## Docker

Create the image

```
docker build -t trades . 
```

Run docker and open the port

```
docker run -p 5433:5432 trades -d
```
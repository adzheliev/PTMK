<span style="font-family: Tahoma;"> 

### To run locally

Open your IDE of choice and clone this repository

```bash
https://github.com/adzheliev/PTMK.git
```
To run **test for application**, please use following command on yor terminal

```bash
docker compose up -d
```

Once all dependencies are installed you can run following commands from terminal:

```bash
python main.py 1
```
to create a DB table

```bash
python main.py 2 "Zvanov Petr Sergeevich" 2009-07-12 Female  
```
to create insert a single row in DB table

```bash
python main.py 3
```
to visualize all unique rows in a DB table

```bash
python main.py 4
```
to insert a 1 million + 100 randomly created rows in a DB table

```bash
python main.py 5
```
to visualize a time of execution of a simple query

```bash
python main.py 6
```
to optimize a DB table by adding indexes



Once you finished testing the application use following command on yor terminal to remove containers
```bash
docker compose down    
```

</span>
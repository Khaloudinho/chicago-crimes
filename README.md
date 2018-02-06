# Title
- chicago-crimes

# Author
- Khaled BOUGUETTOUCHA

# Description
- Few Hadoop map reduce programs made for managing big amount of data related to crimes that happened in Chicago

# Prerequisites
- Have Java and Maven 
- Have hadoop installed on your laptop

# How to perform it ?
- First, build the project to generate .jar : <br />
``` mvn clean install ``` <br />

- Then, run this command with replacing X by the number of the question you want to see the result : <br />
``` hadoop jar target\chicago-crimes-1.0-SNAPSHOT.jar questionX.Crimes data\Crimes-2001-present.csv out\questionX ```

# Comment
- I do not own the code of the k-means used in the package ``` question3 ```. All right reserved to Andrea Iacono for more informations you can go through the following page : https://github.com/andreaiacono/MapReduce/tree/master/src/main/java/samples/kmeans

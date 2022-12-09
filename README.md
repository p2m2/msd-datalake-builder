# msd-datalake-builder

## FORUM datalake

- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-nlm-mesh
- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-nlm-mesh-vocab
- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-ebi-chebi
- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-ncbi-pubchem-compound-general
- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-ncbi-pubchem-reference
- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-sparontology-cito
- https://services.pfem.clermont.inrae.fr/gitlab/metabosemdatalake/databases/database-ext-sparontology-fabio
- PMID_CID
- PMID_CID_ENDPOINTS


``` 
mkdir /tmp/spark-events

/usr/local/share/spark/bin/spark-submit \
   --conf "spark.eventLog.enabled=true" \
   --conf "spark.eventLog.dir=file:///tmp/spark-events" \
   --executor-memory 1G \
   --num-executors 1 \
   --jars ./sansa.jar \
    assembly/msd-metdisease-request-compound-mesh.jar \
    -n 1 \
    -l src/test/resources/skos-owl1-dl.rdf,src/test/resources/something.rdf,src/test/resources/something.ttl,src/test/resources/something.nt \
    -o ./out_test
```

``` 
spark-submit  \
 --name DatalakeRdfForum \
 --deploy-mode cluster \
 --executor-memory 5g \
 --num-executors 48 \
 --conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
 --conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/"  \
 --conf spark.yarn.submit.waitAppCompletion="false" \
 --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
 msd-metdisease-request-compound-mesh.jar
```

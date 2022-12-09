package fr.inrae.msd.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatalakeBuilderSpec extends AnyFlatSpec with Matchers {
  var spark =
    SparkSession
    .builder()
      .appName("local-test")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .getOrCreate()

  "DatalakeBuilder Build Datalake with 'Something' Example using skos." should "creatre triples" in {

    fr.inrae.msd.rdf.DatalakeBuilder.build(
      spark,
      Seq(
        getClass.getResource("/skos-owl1-dl.rdf").getPath,
        getClass.getResource("/something.nt").getPath,
        getClass.getResource("/something.rdf").getPath,
        getClass.getResource("/something.ttl").getPath,
      ),
      "./output_test",
      n = 1,
      debug = false
    )
/*
    PmidCidWork.getPMIDListFromReference_impl1(
      spark,
      getClass.getResource("/pc_reference_type_test.ttl").getPath).count() shouldEqual 497*/
  }

}

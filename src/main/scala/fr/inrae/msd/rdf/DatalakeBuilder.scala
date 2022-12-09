package fr.inrae.msd.rdf

import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.inference.spark.forwardchaining.triples.ForwardRuleReasonerOWLHorst

object DatalakeBuilder extends App {

  def getLang(path: String) : Lang = path.split("\\.").last match {
    case "ttl" => Lang.TURTLE
    case "nt" => Lang.NT
    case "rdf" => Lang.RDFXML
    case "owl" => Lang.RDFXML
    case ext =>  System.err.println(s"Unknown extension :$ext") ; Lang.TURTLE
  }
  import scopt.OParser

  case class Config(
                   outputPath : String = "/rdf-test/forum-inference-CHEBI-PMID_partition",
                   rdfSources : Seq[String] = Seq(
                     "/rdf/forum/DiseaseChem/PMID_CID/2022-06-08_2022-07-07-090250/pmid_cid_partition_40.ttl",
                     "/rdf/forum/DiseaseChem/PMID_CID/2022-06-08_2022-07-07-090250/pmid_cid_endpoints_partition_64.ttl",
                     "/rdf/nlm/mesh/SHA_5a785145/mesh.nt",
                     "/rdf/nlm/mesh-ontology/0.9.3/vocabulary_0.9.ttl",
                     "/rdf/ebi/chebi/13-Jun-2022/chebi.owl",
                     "/rdf/pubchem/compound-general/2022-06-08/pc_compound_type.ttl",
                     "/rdf/pubchem/reference/2022-06-08/pc_reference_type.ttl",
                     "/rdf/sparontology/cito/2.8.1/cito.ttl",
                     "/rdf/sparontology/fabio/2.1/fabio.ttl"
                   ),
                   nPartition: Int = 64,
                   debug: Boolean = false)

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("msd-datalake-builder"),
      head("msd-datalake-builder", "1.0"),
      opt[Seq[String]]('l', "rdfSources")
        .optional()
        .valueName("<rdfSources>")
        .action((x, c) => c.copy(rdfSources = x))
        .text(s"nPartition : build n partition (default ${Config().rdfSources.mkString(",")})"),
      opt[String]('o', "outputPath")
        .optional()
        .valueName("<outputPath>")
        .action((x, c) => c.copy(outputPath = x))
        .text(s"nPartition : build n partition (default ${Config().outputPath})"),
      opt[Int]('n', "nPartition")
        .optional()
        .valueName("<nPartition>")
        .action((x, c) => c.copy(nPartition = x))
        .text(s"nPartition : build n partition (default ${Config().nPartition})"),
      opt[Unit]("debug")
        .hidden()
        .action((_, c) => c.copy(debug = true))
        .text("this option is hidden in the usage text"),

      help("help").text("prints this usage text"),
      note("some notes." + sys.props("line.separator")),
      checkConfig(_ => success)
    )
  }
  
  val sansaRegistrator : String =
    Seq("net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify").mkString(",")

  val spark = SparkSession
    .builder()
    .appName("msd-chemdisease-datalake")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.kryo.registrator",sansaRegistrator)
    .getOrCreate()

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        build(spark,config.rdfSources,config.outputPath,config.nPartition,config.debug)
      case _ =>
        // arguments are bad, error message will have been displayed
        System.err.println("exit with error.")
    }

  /**
   * @param debug activation of debug mode
   */
  def build(
             spark : SparkSession,
             rdfSources : Seq[String],
             outputPath : String,
             n: Int,
             debug: Boolean) : Unit = {

//    val startBuild = new Date()

    val triples: RDD[Triple] = rdfSources.foldLeft(spark.sparkContext.emptyRDD[Triple])
        {
          case (rdd : RDD[Triple], path : String)
            => rdd.union(spark.rdf(getLang(path))(path))
        }

    {
      import net.sansa_stack.rdf.spark.model.TripleOperations
      val inferredTriples = triples.toDS().coalesce(n).rdd
      new ForwardRuleReasonerOWLHorst(spark.sparkContext)
        .apply(inferredTriples)
        .toDS()
        .coalesce(n).rdd
    }.saveAsNTriplesFile(s"$outputPath/$n",mode=SaveMode.Overwrite)

    println("FIN")
/*
    val contentProvenanceRDF : String =
      ProvenanceBuilder.provSparkSubmit(
      projectUrl ="https://github.com/p2m2/msd-datalake-builder",
      category = forumCategoryMsd,
      database = forumDatabaseMsd,
      release="test",
      startDate = startBuild,
      spark
    )

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category="prov",
      database="",
      version=Some("")).writeFile(spark,contentProvenanceRDF,"msd-datalake-builder.ttl")

    spark.close()
    */
  }

}

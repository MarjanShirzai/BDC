""""
Vragen:
1.Hoeveel "features" heeft een Archaea genoom gemiddeld?
2.Hoe is de verhouding tussen coding en non-coding features? (Deel coding door non-coding totalen).
3.Wat zijn de minimum en maximum aantal eiwitten van alle organismen in het file?
4.Verwijder alle non-coding (RNA) features en schrijf dit weg als een apart DataFrame (Spark format).
5.Wat is de gemiddelde lengte van een feature ?

Stappen plan:
1: GBFF file inlezen en omzetten in een pyspark datafram

mshirzai@nuc221:/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea$ head -n 100 archaea.1.genomic.gbff
LOCUS       NZ_SDJP01000015        78255 bp    DNA     linear   CON 19-MAR-2022
DEFINITION  Halorubrum amylolyticum strain ZC67 scaffold15, whole genome
            shotgun sequence.
ACCESSION   NZ_SDJP01000015 NZ_SDJP01000000
VERSION     NZ_SDJP01000015.1
DBLINK      BioProject: PRJNA224116
            BioSample: SAMN10782550
            Assembly: GCF_004114995.1
KEYWORDS    WGS; RefSeq.
SOURCE      Halorubrum amylolyticum
  ORGANISM  Halorubrum amylolyticum
            Archaea; Euryarchaeota; Stenosarchaea group; Halobacteria;
            Haloferacales; Halorubraceae; Halorubrum.
REFERENCE   1  (bases 1 to 78255)
  AUTHORS   Chen,S.
  TITLE     An extremely halophilic archaeon isolated from a subterranean rock
            salt
  JOURNAL   Unpublished
REFERENCE   2  (bases 1 to 78255)
  AUTHORS   Chen,S.
  TITLE     Direct Submission
  JOURNAL   Submitted (23-JAN-2019) Department of Biology, Anhui University,
            No. 1 Beijing East Road, Wuhu, Anhui 241000, China
COMMENT     REFSEQ INFORMATION: The reference sequence is identical to
            SDJP01000015.1.
            The annotation was added by the NCBI Prokaryotic Genome Annotation
            Pipeline (PGAP). Information about PGAP can be found here:
            https://www.ncbi.nlm.nih.gov/genome/annotation_prok/

            ##Genome-Annotation-Data-START##
            Annotation Provider               :: NCBI RefSeq
            Annotation Date                   :: 03/19/2022 03:19:07
            Annotation Pipeline               :: NCBI Prokaryotic Genome
                                                 Annotation Pipeline (PGAP)
            Annotation Method                 :: Best-placed reference protein
                                                 set; GeneMarkS-2+
            Annotation Software revision      :: 6.0
            Features Annotated                :: Gene; CDS; rRNA; tRNA; ncRNA;
                                                 repeat_region
            Genes (total)                     :: 3,665
            CDSs (total)                      :: 3,601
            Genes (coding)                    :: 3,441
            CDSs (with protein)               :: 3,441
            Genes (RNA)                       :: 64
            rRNAs                             :: 1, 1, 1 (5S, 16S, 23S)
            complete rRNAs                    :: 1, 1, 1 (5S, 16S, 23S)
            tRNAs                             :: 59
            ncRNAs                            :: 2
            Pseudo Genes (total)              :: 160
            CDSs (without protein)            :: 160
            Pseudo Genes (ambiguous residues) :: 0 of 160
            Pseudo Genes (frameshifted)       :: 53 of 160
            Pseudo Genes (incomplete)         :: 127 of 160
            Pseudo Genes (internal stop)      :: 24 of 160
            Pseudo Genes (multiple problems)  :: 37 of 160
            CRISPR Arrays                     :: 4
            ##Genome-Annotation-Data-END##
FEATURES             Location/Qualifiers
     source          1..78255
                     /organism="Halorubrum amylolyticum"
                     /mol_type="genomic DNA"
                     /submitter_seqid="scaffold15"
                     /strain="ZC67"
                     /isolation_source="Salt mine"
                     /type_material="type material of Halorubrum amylolyticum"
                     /db_xref="taxon:2508724"
                     /country="China: Yunnan"
                     /collection_date="2013-09-10"
                     /collected_by="Shaoxing Chen"
     gene            <1..460
                     /locus_tag="ESO89_RS11020"
     CDS             <1..460
                     /locus_tag="ESO89_RS11020"
                     /inference="COORDINATES: similar to AA
                     sequence:RefSeq:WP_004595976.1"
                     /note="Derived by automated computational analysis using
                     gene prediction method: Protein Homology."
                     /codon_start=2
                     /transl_table=11
                     /product="halocyanin domain-containing protein"
                     /protein_id="WP_128905559.1"
                     /translation="GGSGGGGGSDGSDGSDGGDGGSDGSDGSDGGSGGQEYLSEEPNY
                     DGFLDDVSNYDGTVDMRDADEVTVDVGANDGLTFGPAAVAVSSGTTVVWEWVGQGGDH
                     NVSGSDGSFESDTVGEEGHTFEYTFEESGTYTYVCTPHEAVGMKGAVYVE"
     gene            543..1370
                     /locus_tag="ESO89_RS11025"
     CDS             543..1370
                     /locus_tag="ESO89_RS11025"


+------+----------------+-------------+-----------------+--------+-----------+
|EndPos|       ProteinID|Proteinlength|       SequenceID|StartPos|featureType|
+------+----------------+-------------+-----------------+--------+-----------+
|   460|            NULL|          460|NZ_SDJP01000015.1|       0|       gene|
|   460|[WP_128905559.1]|          460|NZ_SDJP01000015.1|       0|        CDS|
|  1370|            NULL|          828|NZ_SDJP01000015.1|     542|       gene|
|  1370|[WP_128905560.1]|          828|NZ_SDJP01000015.1|     542|        CDS|
|  2671|            NULL|         1296|NZ_SDJP01000015.1|    1375|       gene|
|  2671|[WP_128905561.1]|         1296|NZ_SDJP01000015.1|    1375|        CDS|
|  3242|            NULL|          312|NZ_SDJP01000015.1|    2930|       gene|
|  3242|[WP_128905562.1]|          312|NZ_SDJP01000015.1|    2930|        CDS|
|  4265|            NULL|          942|NZ_SDJP01000015.1|    3323|       gene|
|  4265|[WP_128905563.1]|          942|NZ_SDJP01000015.1|    3323|        CDS|
|  5782|            NULL|         1521|NZ_SDJP01000015.1|    4261|       gene|
|  5782|[WP_128905564.1]|         1521|NZ_SDJP01000015.1|    4261|        CDS|
|  6609|            NULL|          738|NZ_SDJP01000015.1|    5871|       gene|
|  6609|[WP_128905565.1]|          738|NZ_SDJP01000015.1|    5871|        CDS|
|  8200|            NULL|         1359|NZ_SDJP01000015.1|    6841|       gene|
|  8200|[WP_128905566.1]|         1359|NZ_SDJP01000015.1|    6841|        CDS|
|  8948|            NULL|          624|NZ_SDJP01000015.1|    8324|       gene|
|  8948|[WP_128905567.1]|          624|NZ_SDJP01000015.1|    8324|        CDS|
| 10123|            NULL|         1083|NZ_SDJP01000015.1|    9040|       gene|
| 10123|[WP_128905568.1]|         1083|NZ_SDJP01000015.1|    9040|        CDS|
+------+----------------+-------------+-----------------+--------+-----------+

Bronnen:
https://glow.readthedocs.io/en/latest/etl/gff.html
https://biopython.org/docs/1.76/api/Bio.GenBank.Record.html
https://biopython.org/docs/1.75/api/Bio.SeqFeature.html
https://biopython.org/docs/1.75/api/Bio.SeqIO.html

https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/file-formats/annotation-files/about-ncbi-gbff/
https://www.ncbi.nlm.nih.gov/genbank/samplerecord/

https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html
https://sparkbyexamples.com/pyspark/pyspark-aggregate-functions/

"""

from Bio import SeqIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from pyspark.sql.functions import avg, min, max


spark = SparkSession.builder.appName('assignment5').master('local[16]').config('spark.executor.memory','128g').config('spark.driver.memory','128g').getOrCreate()

file = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff"

print("STARTING the processsss")

#features Gene; CDS; rRNA; tRNA; ncRNA;
information = []
for data in SeqIO.parse(file, "gb"):
    #print(data.features)
    #print(data.id)
    for info in data.features:
        if info.type in ["gene", "CDS", "rRNA", "tRNA", "ncRNA"]:
            length = int(info.location.end) - int(info.location.start)
            # SequenceID= data.id,
            # featureType = info.type,
            # StartPos = int(info.location.start),
            # EndPos = int(info.location.end),
            # ProteinID= info.qualifiers.get("protein_id")
            selectedInfo = {
                "SequenceID": data.id,
                "featureType": info.type,
                "StartPos": int(info.location.start),
                "EndPos": int(info.location.end),
                "ProteinID": info.qualifiers.get("protein_id"),
                "Proteinlength": length
            }
            information.append(selectedInfo)




print("GELUKT: Lijst aangemaakt met alle features uit het bestand")
pysparkData = spark.createDataFrame(information)
print("GELUKT: Data frame aangemaakt! ")
print("Dataframe eerste 20 rijen: ")
pysparkData.show(20)


"""
Vraag 1: Hoeveel "features" heeft een Archaea genoom gemiddeld?

Omdat ik niet zeker wist of ik 1 gbff file moet gebruiken of meerdere in de directory heb ik de archaea.1.genomic.gbff
geselecteerd. Het gaat hier om het organisme Halorubrum amylolyticum. 

De vraag was niet duidelijk voor mij, Ik interpreteer het als bereken voor verschillende bestanden feature en deel door
aantal om gemiddelde te berekenen. 

Omdat het een HPC vak ik heb ik de berekening als volgd gedaan om te laten zien dat ik pyspark snap:
Ik tel allee sequentie id om de aantal genetische info te tellen en daarvan de gemiddelde te nemen. 

"""
print("---------------------------------------------")
#print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
totalSeqID = pysparkData.groupBy("SequenceID").count()
totalSeqID.show(5)
avarage =  totalSeqID.agg(sum("count")).collect()[0][0]
print("vraag 1: {} ".format(avarage))
print("---------------------------------------------")

""""
features Gene; CDS; rRNA; tRNA; ncRNA;
Vraag 2: Hoe is de verhouding tussen coding en non-coding features? (Deel coding door non-coding totalen)
Coding gedeelte is CDS non coding is rest 

Coding features zijn met de "CDS" gemarkeerd
Non-coding features zijn met de "ncRNA" of "rRNA" tags gemarkeerd, of,
"Genes" die geen "CDS" bevatten zijn ook non-coding (cryptic genes)
Pro-peptides, als je ze tegenkomt, zijn "coding".

"""

print("---------------------------------------------")
featureType = pysparkData.groupBy("featureType").count()
featureType.show()

#Selecteren CDS
#CDS = featureType.filter(col("featureType") == "CDS").select("count").collect()
CDS = featureType.filter(col("featureType") == "CDS").select("count").collect()[0][0]
#print("CDS count {}". format(CDS))
#1424223

gene = featureType.filter(col("featureType") == "gene").select("count").collect()[0][0]
rRNA = featureType.filter(col("featureType") == "rRNA").select("count").collect()[0][0]
tRNA = featureType.filter(col("featureType") == "tRNA").select("count").collect()[0][0]
ncRNA = featureType.filter(col("featureType") == "ncRNA").select("count").collect()[0][0]

nonCoding = gene + rRNA + tRNA +ncRNA

ratio = CDS/nonCoding

print("---------------------------------------------")
print("Vraag 2 ratio coding versus non coding: {}".format(ratio))
print("---------------------------------------------")

""""

Vraag 3: Wat zijn de minimum en maximum aantal eiwitten van alle organismen in het file?
Voor minimum en maximum eiwitten heb ik tijdens het lezen van de data file brekening gemaakt:
Eind - Start om lengte te berekenen en toegevoegd in Proteinlength kolom.
"""

#df.select(min("salary")).show(truncate=False)

print("Table below show MINIMUM protein length ")
pysparkData.select(min("Proteinlength")).show(truncate=False)

print("Table below show MAXIMUM protein length ")
pysparkData.select(max("Proteinlength")).show(truncate=False)

print("---------------------------------------------")

"""

Vraag 4: Verwijder alle non-coding (RNA) features en schrijf dit weg als een apart DataFrame (Spark format)

Uit gaand van dit info schrijf ik het weg naar een CSV: 
What is Spark format?
Apache Spark is a big data processing framework that can be used to read and write data for CSV, JSON, Parquet and Delta Lake

"""

#df.write.parquet('bar.parquet')
#spark.read.parquet('bar.parquet').show()
print("---------------------------------------------")

nonCoding2 = pysparkData.filter(col("featureType").isin("rRNA", "tRNA", "ncRNA", "gene"))
print("Vraag 4: Eerste 5 rijen van de nonCoding Dataframe: ")

nonCoding2.write.parquet('NoneCoding.parquet')

print("The check to see if its readble")
spark.read.parquet('NoneCoding.parquet').show(5)


"""

Vraag 5: Wat is de gemiddelde lengte van een feature ?

"""

print("---------------------------------------------")
print("Vraag 5 Gemmidelse lengthe: ")
gemLengte = pysparkData.agg(avg("Proteinlength"))
gemLengte.show()
print("---------------------------------------------")

print("KLAAR!")
spark.stop()

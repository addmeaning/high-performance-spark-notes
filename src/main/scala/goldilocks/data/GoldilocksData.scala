package goldilocks.data

/**
  * description: 
  * link to test/main class:
  */
object GoldilocksData {

  val exampleData = List(
    ("Mama Panda", 15.0, 0.25, 2467.0, 0.0),
    ("Papa Panda", 2.0, 1000, 35.4, 0.0),
    ("Baby Panda", 10.0, 2.0, 50.0, 0.0),
    ("Baby Panda’s toy Panda", 3.0, 8.5, 0.2, 98.0))

  val exampleResult = List(
    (1, List(3.0, 15.0)),
    (2, List(2.0, 1000.0)),
    (3, List(35.4, 2467.0)),
    (4, List(0.0, 98.0)))
}

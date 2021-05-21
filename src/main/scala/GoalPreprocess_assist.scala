
object GoalPreprocess_assist {

  val regDate =  (DateRough:String,interaction_type:String) => {
    val arr = DateRough.split("\\.?") match {
      case correct@Array(_,_,_,_,_,_,_,_,_,_,_,_) => correct
      case _  => throw new Exception("Array must have strictly 12 elements while parsing date format like 197001011450")
    }

    val year:String = arr.take(4).toList.mkString("")

    val other:Array[String] = arr.drop(4)
    val arr_other:List[String] = other.sliding(2,2).toList.map(_.mkString(""))
    val List(month,day,hour,minute) = arr_other.toList
    val second:String = interaction_type match {
      case "goal" => "40"
      case _      => "30"
    }

    val date:String = List(year,month,day).mkString("-") + " " + List(hour,minute,second).mkString(":")

    date

  }

}
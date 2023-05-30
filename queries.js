// jobs queries

// 1.
db.NYC_Jobs.distinct("Career Level")

// 2.
db.NYC_Jobs.find({ 
    "Posting Type": "External",
    "Salary Frequency": "Hourly"
  },
  {
    "Agency": 1,
    "Business Title": 1,
    "Salary Range To": 1
  }).sort({"Salary Range To": -1}).limit(3)

// 3.

db.NYC_Jobs.aggregate([
  {
    $match: {
      "Posting Type": "External",
      "Full-Time/Part-Time indicator": "F"
    }
  },
  {
    $group: {
      _id: "$Agency",
      count: { $sum: 1 }
    }
  },
  {
    $sort: {
      count: -1
    }
  }
])


// 4.
db.NYC_Jobs.aggregate([
  {
    $match: {
      "Posting Type": "External",
      "Full-Time/Part-Time indicator": "F"
    }
  },
  {
    $group: {
      _id: "$Agency",
      count: { $sum: 1 }
    }
  },
  {
    $match: {
      count: { $gt: 100 }
    }
  },
  {
    $sort: {
      count: -1
    }
  }
])


// 5.
db.NYC_Jobs.aggregate([
  {
    $match: {
      "Posting Type": "External",
      "Full-Time/Part-Time indicator": "F"
    }
  },
  {
    $group: {
      _id: "$Agency",
      count: { $sum: 1 }
    }
  },
  {
    $match: {
      count: { $gt: 100 }
    }
  },
  {
    $project: {
      _id: 0,
      count: 1,
      agency: { $toLower: "$_id" }
    }
  },
  {
    $sort: {
      count: -1
    }
  }
])


// 6.
db.NYC_Jobs.aggregate([
  {
    $group: {
      _id: {
        year: { $arrayElemAt: [{ $split: ["$Posting Date", "/"] }, 2] }
      },
      averageSalary: { $avg: "$Salary Range To" }
    }
  },
  {
    $project: {
      _id: 0,
      _idYear: "$_id.year",
      "Average Salary": { $round: ["$averageSalary", 2] }
    }
  },
  {
    $sort: {
      _idYear: 1
    }
  }
])

// 1. Show exactly three documents
db.listings.find().limit(3);

// 2. Show 10 documents, pretty-printed
db.listings.find().limit(10).pretty();

// 3. Two superhosts and all their listings
// Step A: find superhost IDs
db.listings.distinct("host_id", { host_is_superhost: "t" });

// Step B: replace these with two IDs from your dataset
db.listings.find({
  host_id: { $in: ["12345", "67890"] },
  host_is_superhost: "t"
}).pretty();

// 4. Find all values of host_is_superhost
db.listings.distinct("host_is_superhost");

// 5. Places with >2 beds in a chosen neighbourhood group, sorted by rating desc
db.listings.find(
  {
    beds: { $gt: 2 },
    neighborhood_group_cleansed: "Centro"
  },
  {
    name: 1,
    beds: 1,
    neighborhood_group_cleansed: 1,
    review_scores_rating: 1
  }
).sort({ review_scores_rating: -1 });

// 6. Number of listings per host
db.listings.aggregate([
  {
    $group: {
      _id: "$host_id",
      host_name: { $first: "$host_name" },
      listings_count: { $sum: 1 }
    }
  },
  { $sort: { listings_count: -1 } }
]);

// 7. Average rating per neighbourhood, only 95+ rating
db.listings.aggregate([
  {
    $match: {
      review_scores_rating: { $ne: null }
    }
  },
  {
    $group: {
      _id: "$neighborhood_cleansed",
      avg_rating: { $avg: "$review_scores_rating" },
      count_listings: { $sum: 1 }
    }
  },
  {
    $match: {
      avg_rating: { $gte: 95 }
    }
  },
  {
    $sort: { avg_rating: -1 }
  }
]);

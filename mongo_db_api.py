from pymongo import MongoClient

class MongoAPI:

    # Establishes a connection with the database.
    def __init__(self):
        self.client = MongoClient()
        self.client.drop_database("explore_mongo")
        self.db = self.client.explore_mongo
    
    # Loads the given document into the "restaurants" collection in the database.
    def insert_one(self, restaurant):
        self.db.restaurants.insert_one(restaurant)

    # Gets the number of restaurants in a given borough.
    def get_num_restaurants_in_borough(self, borough):
        return self.db.restaurants.count_documents({"borough": borough})

    # Gets the zipcodes with the most restaurants (number of results returned limited by
    # limit parameter).
    def get_zipcodes_most_restaurants(self, limit):
        pipeline = [
            {"$group": {"_id": "$address.zipcode", "restaurant_count": {"$sum": 1}}},
            {"$sort": {"restaurant_count": -1}},
            {"$limit": limit}
        ]
        return list(self.db.restaurants.aggregate(pipeline))
    
    # Gets the number of restaurants for a given cuisine that have a given grade.
    def get_num_restaurants_of_grade(self, grade, cuisine):
        pipeline = [
            {"$unwind": "$grades"},
            {"$match": {"cuisine": cuisine, "grades.grade": grade}},
            {"$group": {"_id": None, "count": {"$sum": 1}}}
        ]
        result = list(self.db.restaurants.aggregate(pipeline))
        if result:
            return result[0]["count"]
        else:
            return 0
        
    # Get the min and max score values for each grade.
    def get_grade_score_range(self, grade):
        pipeline = [
            {"$match": {"grades.grade": grade}},
            {"$unwind": "$grades"},
            {"$match": {"grades.grade": grade}},
            {"$group": {"_id": grade, "min_score": {"$min": "$grades.score"}, "max_score": {"$max": "$grades.score"}}}
        ]
        result = list(self.db.restaurants.aggregate(pipeline))
        if result:
            return result[0]
        
    # Get the most popular cuisine (determined by number of restaurants) for each borough.
    def get_most_popular_cuisine_per_borough(self):
        pipeline = [
            {"$group": {"_id": {"borough": "$borough", "cuisine": "$cuisine"}, "count": {"$sum": 1}}},
            {"$sort": {"_id.borough": 1, "count": -1}},
            {"$group": {"_id": "$_id.borough", "most_popular_cuisine": {"$first": "$_id.cuisine"}, "count": {"$first": "$count"}}}
        ]
        return list(self.db.restaurants.aggregate(pipeline))
    
    # Get the most popular cuisines overall (including all boroughs, and those not labelled with a borough).
    def get_most_popular_cuisines_overall(self, limit):
        pipeline = [{"$group": {"_id": "$cuisine", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": limit}]
        return list(self.db.restaurants.aggregate(pipeline))

    # Given a borough, get a list of the most frequent cuisines (result can be limited).
    def get_top_cuisines_for_borough(self, borough, limit):
        pipeline = [
            {"$match": {"borough": borough}},
            {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        return list(self.db.restaurants.aggregate(pipeline))
    
    # Get the average health inspection scores for every borough.
    def get_average_scores_per_borough(self):
        # As grades contains a list of other units, unwind is used to extract each element from that
        # list and turn it into a document for easier processing. Also, must check that only numerical
        # values are being counted in the averages.
        pipeline = [
            {"$match": {"grades": {"$exists": True, "$ne": []}}},
            {"$unwind": "$grades"},
            {"$group": {"_id": "$borough", "avg_score": {"$avg": "$grades.score"}}}
        ]
        return list(self.db.restaurants.aggregate(pipeline))
    
    # Get the restaurants near a specified coordinate within a given max distance.
    def get_nearby_restaurants(self, longitude, latitude, max_distance_meters):
        # Perform a geospatial query to find nearby restaurants.
        self.db.restaurants.create_index({"address.coord": "2dsphere"})
        query = {
            "address.coord": {
                "$nearSphere": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [longitude, latitude]
                    },
                    "$maxDistance": max_distance_meters
                }
            }
        }

        attributes_to_show = {"restaurant_id": 1, "name": 1, "address.coord": 1, "borough": 1, "cuisine": 1}
        
        # Execute the query and return the result.
        nearby_restaurants = self.db.restaurants.find(query, attributes_to_show)
        return list(nearby_restaurants)

    # For a given borough, get all the restaurants that are in a specified cuisine.
    def get_restaurants_of_cuisine(self, cuisine, borough):
        attributes_to_show = {"restaurant_id": 1, "name": 1, "address.coord": 1, "borough": 1, "cuisine": 1}
        restaurants = list(self.db.restaurants.find({"cuisine": cuisine, "borough": borough}, attributes_to_show))
        return restaurants
    



    



       
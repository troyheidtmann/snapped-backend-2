"""
Test Survey Module

This module manages test survey data insertion for development and
testing purposes.

Features:
- Survey data creation
- Client info management
- Response simulation
- Group assignment
- Data validation

Data Model:
- Survey responses
- Client information
- User preferences
- Activity data
- Group memberships

Dependencies:
- MongoDB for storage
- asyncio for async ops
- app.shared.database for DB connection

Author: Snapped Development Team
"""

import asyncio
from app.shared.database import async_client, survey_responses

async def insert_test_survey():
    """
    Insert test survey data into database.
    
    Returns:
        None
        
    Notes:
        - Creates document
        - Sets timestamps
        - Handles responses
        - Error handling
        - Closes connection
    """
    try:
        # Test survey document
        test_survey = {
            "timestamp": "2024-02-23T09:09:08.942Z",
            "user_id": "haven_lough",
            "client_id": "haven_lough",
            "client_name": "Haven Lough",
            "responses": {
                "client_id": "haven_lough",
                "user_id": "haven_lough",
                "username": "haven_lough",
                "email": "havenlough@gmail.com",
                "responses": {
                    "overall_have_you_seen_an_increase_or_decrease_with": "increase",
                    "how_much_did_you_earn_estimate_from_your_highest_g": "6000-15000",
                    "what_type_of_content_do_you_share_most_often_on_st": "Lifestyle and talking to the camera about my life",
                    "what_is_your_average_story_view_time_in_the_past_2": "100K per slide, 1-2 million for the day",
                    "how_many_minutes_of_snapchat_content_are_you_posti": "20-40 snaps",
                    "do_you_experiment_with_snapchat_features": [],
                    "how_do_you_engage_with_your_snapchat_followers": ["replies"],
                    "what_feedback_have_you_received_from_friends_or_fo": "",
                    "what_is_the_typical_length_of_your_spotlight_video": [],
                    "how_many_followers_did_you_gain_from_your_highest_": "400K+", 
                    "what_types_of_spotlight_videos_think_tiktok_ig_ree": "",
                    "what_types_of_content_do_you_typically_post_on_spo": "",
                    "how_did_you_originally_gain_your_following_on_snap": "Yes, all platforms via bio and sometimes video.",
                    "what_does_a_typical_day_in_your_life_look_like_fro": "Cooking, School, Soccer, Pickleball, Gym, Hanging with friends, Errands, Work",
                    "what_are_your_favorite_parts_of_your_daily_routine": "Working out, whether it is a sport or the gym.",
                    "what_tone_or_style_do_you_aim_for_in_your_snapchat": ["Personal", "Happy", "Honest"],
                    "what_activities_do_you_like_to_record_with_your_ph": "",
                    "how_would_your_friends_or_family_describe_your_per": "High energy",
                    "is_there_anything_about_your_life_you_feel_is_unde": "My dermatologist journey",
                    "are_there_specific_skills_or_activities_youre_curr": "Basketball",
                    "what_types_of_places_do_you_like_to_visit_regularl": "I love the beach, and any place where I can play a sport",
                    "do_you_prefer_a_fastpaced_or_laidback_lifestyle_wh": "",
                    "name_at_least_3_challenges_you_face_throughout_the": "Homework, business problems, making content",
                    "what_activities_do_you_like_to_do_with_friends_": "Sports like pickleball, make content, and eat",
                    "name_some_influencers_who_inspired_you_to_start_cr": "",
                    "what_type_of_content_do_you_enjoy_creating_most_eg": "Lifestyle and talking to the camera about my life"
                }
            },
            "groups": ["CREATOR"]
        }
        
        # Insert the document
        result = await survey_responses.insert_one(test_survey)
        print(f"Successfully inserted test survey with ID: {result.inserted_id}")
    except Exception as e:
        print(f"Error inserting test survey: {str(e)}")
    finally:
        # Close the client
        async_client.close()

if __name__ == "__main__":
    # Run the async function
    asyncio.run(insert_test_survey()) 
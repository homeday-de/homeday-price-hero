geo_response_10315 = {
    'items': {
        'aviv': [{
            'match': {
                "id": "NBH2DE75702",
                "type_key": "NBH2",
                "coordinates": {"lat": 52.50339854556861, "lng": 13.518376766536123},
                "bounding_box": {"ne": {"lat": 52.491, "lng": 13.4964}, "sw": {"lat": 52.5165, "lng": 13.5432}},
                "match_name": "Friedrichsfelde",
                "confidence_score": 1,
                "parents": []
            }
        }]
    }
}

geo_response_12589 = {
    'items': {
        'aviv': [{
            'match': {
                "id": "NBH2DE75693",
                "type_key": "NBH2",
                "coordinates": {"lat": 52.44183420284346, "lng": 13.705967663911409},
                "bounding_box": {"ne": {"lat": 52.4167, "lng": 13.6509}, "sw": {"lat": 52.4683, "lng": 13.7611}},
                "match_name": "Rahnsdorf",
                "confidence_score": 1,
                "parents": []
            }
        }]
    }
}

geo_response_Ohne = {
    "items": {
        "aviv": [{
            "match": {
                "id": "AD08DE1992",
                "type_key": "AD08",
                "coordinates": {
                    "lat": 52.273521665147335,
                    "lng": 7.27936823510655
                },
                "bounding_box": {
                    "ne": {
                    "lat": 52.2638103,
                    "lng": 7.218268
                    },
                    "sw": {
                    "lat": 52.2900363,
                    "lng": 7.3173127
                    }
                },
                "match_name": "Ohne",
                "confidence_score": 1,
                "parents": []
            }
        }]
    }
}

geo_response_Ködnitz = {
    "items": {
        "aviv": [{
            "match": {
            "id": "AD08DE7589",
            "type_key": "AD08",
            "coordinates": {
                "lat": 50.099291599932535,
                "lng": 11.510340529692305
            },
            "bounding_box": {
                "ne": {
                "lat": 50.0674009,
                "lng": 11.4781884
                },
                "sw": {
                "lat": 50.1339989,
                "lng": 11.5444341
                }
            },
            "match_name": "Ködnitz",
            "confidence_score": 1,
            "parents": []
            }
        }]
    }
}

price_response_NBH2DE75702 = {
    "items": [{
        "place_id": "NBH2DE75702",
        "price_date": "2023-10-01",
        "transaction_type": "TRANSACTION_TYPE.SELL",
        "house_price": {"place_id": "NBH2DE75702", "low": 2011, "high": 10053, "value": 5027, "accuracy": 2},
        "apartment_price": {"place_id": "NBH2DE75702", "low": 2146, "high": 6820, "value": 3940, "accuracy": 3},
        "hybrid_price": {"place_id": "NBH2DE75702", "low": 2145, "high": 6832, "value": 3944, "accuracy": 3}
    }],
    "query_duration": 0.751335859298706
}

price_response_NBH2DE75693 = {
  "items": [{
        "place_id": "NBH2DE75693",
        "price_date": "2023-10-01",
        "transaction_type": "TRANSACTION_TYPE.SELL",
        "house_price": {"place_id": "NBH2DE75693", "low": 3378, "high": 6225, "value": 4404, "accuracy": 4},
        "apartment_price": None,
        "hybrid_price": {"place_id": "NBH2DE75693", "low": 3371, "high": 6214, "value": 4396, "accuracy": 4}
    }],
  "query_duration": 0.09646821022033691
}


geo_responses = {'10315': geo_response_10315, '12589': geo_response_12589, 'Ohne': geo_response_Ohne, 'Ködnitz': geo_response_Ködnitz}
price_responses = {'NBH2DE75702': price_response_NBH2DE75702, 'NBH2DE75693': price_response_NBH2DE75693}

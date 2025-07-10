def archive_inactive_users():
    # Текущая дата и расчет временных рамок
    today = datetime.now()
    registration_threshold = today - timedelta(days=30)
    activity_threshold = today - timedelta(days=14)

    # Формирование запроса для поиска неактивных пользователей
    pipeline = [
        {
            "$match": {
                "user_info.registration_date": {"$lt": registration_threshold},
                "event_time": {"$lt": activity_threshold}
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "last_event_time": {"$max": "$event_time"},
                "email": {"$first": "$user_info.email"}
            }
        },
        {
            "$match": {
                "last_event_time": {"$lt": activity_threshold}
            }
        }
    ]

    # Поиск пользователей для архивации
    inactive_users = list(user_events.aggregate(pipeline))

    if not inactive_users:
        print("Нет неактивных пользователей для архивации")
        return

    # Архивация пользователей
    archived_count = 0
    for user in inactive_users:
        user_doc = {
            "user_id": user["_id"],
            "email": user["email"],
            "archived_date": today
        }
        archived_users.insert_one(user_doc)
        user_events.delete_many({"user_id": user["_id"]})
        archived_count += 1

    # Сохранение отчета
    report_data = {"archived_count": archived_count}
    report_filename = today.strftime("%Y-%m-%d") + ".json"

    with open(report_filename, "w") as report_file:
        json.dump(report_data, report_file, indent=4)

    print(f"Архивировано {archived_count} пользователей. Отчет сохранен в {report_filename}")

    if __name__ == "__main__":
        archive_inactive_users()
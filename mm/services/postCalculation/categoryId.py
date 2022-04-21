from redisHandler import redisHandler


class categoryId:
    def __init__(self, db) -> None:
        self.db = db
        self.redis = redisHandler()

        self.updateRedisCategoryId()

    def updateRedisCategoryId(self):
        pass

    def getCategoryId(self, make, model):

        categoryId = None

        self.db.connect()

        temp_mk_model = make.lower()

        if model:
            temp_mk_model = temp_mk_model + "_" + model.lower()

        temp_mk_model = temp_mk_model.replace("-", "_")
        temp_mk_model = temp_mk_model.replace(" ", "_")

        category_temp = self.redis.get("categoryId-" + temp_mk_model)

        print(category_temp)
        
        if category_temp != None:
            categoryId = category_temp
        else:
            temp_cat_rows = self.db.recSelect("fl_categories", {"Key": temp_mk_model})
            print(temp_cat_rows)
            if len(temp_cat_rows) != 0:
                categoryId = temp_cat_rows[0]["ID"]
            else:
                temp_make_model_path = make

                if model:
                    if temp_make_model_path:
                        temp_make_model_path = temp_make_model_path + "/" + model
                    else:
                        temp_make_model_path = model

                temp_make_model_path = temp_make_model_path.replace(" ", "-")

                make_temp = make

                make_temp = make_temp.replace("-", "_")

                make_temp = make_temp.replace(" ", "_")

                # check the parent category exists in database

                category_rows = self.db.recSelect("fl_categories", {"Key": make_temp})

                if len(category_rows):
                    category_dict = dict()
                    category_dict["Path"] = temp_make_model_path
                    category_dict["Parent_ID"] = category_rows[0]["ID"]
                    category_dict["Parent_IDs"] = category_rows[0]["ID"]
                    category_dict["Parent_keys"] = category_rows[0]["Key"]
                    category_dict["Type"] = category_rows[0]["Type"]
                    category_dict["Key"] = temp_mk_model
                    category_dict["self_inserted"] = 1

                    new_category_id = self.db.recInsert("fl_categories", category_dict)

                    if new_category_id:
                        ###############
                        # push new key to category cache
                        self.redis.set("categoryId-" + temp_mk_model, new_category_id)
                        ###############
                        new_category_dict = {}
                        new_category_dict["Tree"] = (
                            str(category_rows[0]["ID"]) + "." + str(new_category_id)
                        )
                        new_category_dict["Position"] = new_category_id
                        new_category_dict["Level"] = 1
                        new_category_dict["Modified"] = {"func": "now()"}
                        new_category_dict["neglect"] = 0

                        self.db.recUpdate(
                            "fl_categories", new_category_dict, {"ID": new_category_id}
                        )

                        lang_key = "categories+name+" + temp_mk_model

                        temp_value = ""

                        if model:
                            temp_value = model
                        else:
                            temp_value = make

                        self.db.recInsert(
                            "fl_lang_keys",
                            {
                                "Code": "en",
                                "Module": "common",
                                "Key": lang_key,
                                "Value": temp_value,
                            },
                        )

                        categoryId = new_category_id
                else:
                    temp_cat_dict = {
                        "path": make_temp,
                        "level": 0,
                        "Parent_ID": 0,
                        "Parent_IDs": "",
                        "Parent_keys": "",
                        "Key": make_temp,
                        "Name": make,
                        "status": "active",
                        "self_inserted": 2,
                    }

                    new_cate_id = self.db.recInsert("fl_categories", temp_cat_dict)

                    if new_cate_id:
                        self.redis.set("categoryId-" + make_temp, new_cate_id)

                        self.db.recUpdate(
                            "fl_categories",
                            {
                                "neglect": 0,
                                "Position": new_cate_id,
                                "Tree": new_cate_id,
                            },
                            {"ID": new_cate_id},
                        )

                        self.db.recInsert(
                            "fl_lang_keys",
                            {
                                "Code": "en",
                                "Module": "common",
                                "Key": "categories+name+" + make_temp,
                                "Value": make_temp,
                            },
                        )

                        categoryId = new_cate_id

        self.db.disconnect()

        return int(categoryId)

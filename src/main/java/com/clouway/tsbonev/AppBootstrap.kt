package com.clouway.tsbonev

import com.google.appengine.api.datastore.*
import com.google.appengine.api.datastore.FetchOptions.Builder.withLimit
import com.google.appengine.repackaged.com.google.gson.Gson
import spark.*
import spark.Spark.get
import spark.kotlin.*
import spark.servlet.SparkApplication

class AppBootstrap : SparkApplication {

    fun getPerson(req: Request, res: Response): String{

        val id = req.params("key").toLong()

        val key = KeyFactory.createKey("Person", id)

        return JsonTransformer().render(
                PersonRepo().get(key))

    }

    override fun init() {

        //encoding filter
        before(Filter { req, res ->
            res.raw().characterEncoding = "UTF-8"
        })

        get("/countPersons"){
            req, res ->
            PersonRepo().getAll().size
        }

        post("/addPerson", DEFAULT_ACCEPT){
            val p: Person = JsonTransformer().from(request.body(), Person::class.java)
            PersonRepo().save(p)
        }

        get("/viewPerson/:key") {
            req, res ->
                getPerson(req, res)
        }

        get("/viewAllPeople", "application/json", Route { req, res ->
            return@Route JsonTransformer().render(
                    PersonRepo().getAll()
            )
        }, JsonTransformer())

        get("/") { req, res -> "Hello Spark on GAE World" }

        get("/hello") { req, res -> "Hello World" }

    }

}


class Person (val keyId: Long, val fname: String, val lname: String)

class PersonRepo(val dss: DatastoreService = DatastoreServiceFactory.getDatastoreService()){

    fun save(person: Person): Key {
        val entity = Entity("Person", person.keyId)

        entity.setProperty("fname", person.fname)
        entity.setProperty("lname", person.lname)

        dss.put(entity)

        return entity.key
    }

    fun get(key: Key): Person {

        val entity = dss.get(key)
        val person = Person(keyId = entity.key.id, fname = entity.properties["fname"].toString(),
                lname = entity.properties["lname"].toString())

        return person

    }

    fun getAll(): MutableList<Person> {

        val entityList = dss.prepare(Query("Person")).asList(withLimit(10))
        val personList = mutableListOf<Person>()
        for (person in entityList){
            personList.add(Person(person.key.id,
                    person.properties["fname"].toString(),
                    person.properties["lname"].toString()))
        }
        return personList
    }

}

//  json
class JsonTransformer : ResponseTransformer {

    private val gson = Gson()

    override fun render(model: Any): String {
        return gson.toJson(model)
    }

    fun <T> from(json: String?, clazz: Class<T>): T {
        return gson.fromJson<T>(json, clazz)
    }
}
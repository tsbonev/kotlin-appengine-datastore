package com.clouway.tsbonev

import com.google.appengine.api.datastore.*
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig
import com.google.appengine.tools.development.testing.LocalServiceTestHelper
import org.junit.After
import org.junit.Before
import com.google.appengine.api.datastore.FetchOptions.Builder.withLimit
import com.google.appengine.repackaged.com.google.gson.Gson
import com.google.appengine.repackaged.com.google.protobuf.util.JsonFormat
import org.junit.Test
import kotlin.test.assertEquals
import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import spark.ResponseTransformer


class LocalDatastoreTest {

    private val helper = LocalServiceTestHelper(LocalDatastoreServiceTestConfig())

    @Before
    fun setUp() {
        helper.setUp()
    }

    @After
    fun tearDown() {
        helper.tearDown()
    }

    private fun doTest() {
        val ds = DatastoreServiceFactory.getDatastoreService()
        assertEquals(0, ds.prepare(Query("yam")).countEntities(withLimit(10)))
        ds.put(Entity("yam"))
        ds.put(Entity("yam"))
        assertEquals(2, ds.prepare(Query("yam")).countEntities(withLimit(10)))
    }

    @Test
    fun testInsert1() {
        doTest()
    }

    @Test
    fun testInsert2() {
        doTest()
    }

    class JsonTransformer : ResponseTransformer {

        private val gson = Gson()

        override fun render(model: Any): String {
            return gson.toJson(model)
        }

        fun <T> from(json: String?, clazz: Class<T>): T {
            return gson.fromJson<T>(json, clazz)
        }
    }

    @Test
    fun POJOToJSON(){

        val jsonFormat = JsonTransformer()
        val person = Person(1, "John", "Doe")
        assertThat(jsonFormat.render(person) == "{\"keyId\":1,\"fname\":\"John\",\"lname\":\"Doe\"}", Is(true))

    }

    @Test
    fun insertEntityWithValues() {
        val ds = DatastoreServiceFactory.getDatastoreService()
        assertEquals(0, ds.prepare(Query("Person")).countEntities(withLimit(10)))
        val person = Entity("Person", 1234)
        person.setProperty("fname", "John")
        person.setProperty("lname", "Doe")
        ds.put(person)
        assertEquals(1, ds.prepare(Query("Person")).countEntities(withLimit(10)))
    }

    @Test
    fun createKeyAndRetrieveEntity(){
        val ds = DatastoreServiceFactory.getDatastoreService()
        val person = Entity("Person")
        val personKey = person.key
        person.setProperty("fname", "John")
        person.setProperty("lname", "Doe")
        ds.put(person)
        println(personKey)

        val retrievedPerson = ds.get(personKey)
        assertThat(retrievedPerson.getProperty("fname").equals("John"), Is(true))
        assertThat(retrievedPerson.getProperty("lname").equals("Doe"), Is(true))

    }

    @Test
    fun parentHierarchy(){

        val ds = DatastoreServiceFactory.getDatastoreService()

        val johnsDad = Entity("Dad")
        val dadKey = johnsDad.key
        johnsDad.setProperty("fname", "Jon")
        johnsDad.setProperty("lname", "Doe")
        ds.put(johnsDad)


        val john = Entity("Person", dadKey)
        val johnKey = john.key
        john.setProperty("fname", "John")
        john.setProperty("lname", "Doe")
        ds.put(john)

        val johnsSon = Entity("Son", johnKey)
        val sonKey = johnsSon.key
        johnsSon.setProperty("fname", "Joe")
        johnsSon.setProperty("lname", "Doe")
        ds.put(johnsSon)

        println(dadKey)
        println(johnKey)
        println(sonKey)
        println(sonKey.id)

        val retJohn = ds.get(johnKey)
        val retSon = ds.get(sonKey)
        val retDad = ds.get(dadKey)

        val entities = ds.prepare(Query(dadKey)).asList(withLimit(10))
        assertThat(entities.size, Is(3))
        assertThat(entities[0].getProperty("fname").equals("Jon"), Is(true))
        assertThat(entities[1].getProperty("fname").equals("John"), Is(true))
        assertThat(entities[2].getProperty("fname").equals("Joe"), Is(true))

    }

    @Test
    fun noExistingRootKey(){

        val personKey = KeyFactory.createKey("Person", "personKey")

        val ds = DatastoreServiceFactory.getDatastoreService()

        val john = Entity("Person", personKey)
        val johnKey = john.key
        john.setProperty("fname", "John")
        john.setProperty("lname", "Doe")
        ds.put(john)

        val mark = Entity("Person", personKey)
        val markKey = mark.key
        john.setProperty("fname", "Mar")
        john.setProperty("lname", "Roe")
        ds.put(mark)

        println(johnKey)
        println(markKey)

        val personList = ds.prepare(Query(personKey)).asList(withLimit(10))

        assertThat(personList.size, Is(2))
        assertThat(personList[0] == john, Is(true))
    }

    @Test
    fun updateAnEntity(){

        val ds = DatastoreServiceFactory.getDatastoreService()

        val john = Entity("Person")
        val johnKey = john.key
        john.setProperty("fname", "John")
        john.setProperty("lname", "Doe")
        ds.put(john)

        assertThat(ds.get(johnKey).properties["fname"].toString().equals("John"), Is(true))

        val newJohn = Entity(johnKey)
        newJohn.setProperty("fname", "Joe")
        newJohn.setProperty("lname", "Doeson")

        ds.put(newJohn)
        assertThat(ds.get(johnKey).properties["fname"].toString().equals("Joe"), Is(true))
        assertThat(ds.get(johnKey).properties["lname"].toString().equals("Doeson"), Is(true))
        
    }
    
    @Test
    fun getEntityByName(){

        val ds = DatastoreServiceFactory.getDatastoreService()

        val john = Entity("Person")
        val johnKey = john.key
        john.setProperty("fname", "John")
        john.setProperty("lname", "Doe")
        ds.put(john)

        val fnameFilter = Query.FilterPredicate("fname",
                Query.FilterOperator.EQUAL, "John")
        val lnameFilter = Query.FilterPredicate("lname",
                Query.FilterOperator.EQUAL, "Doe")

        val composite = Query.CompositeFilterOperator.and(fnameFilter, lnameFilter)

        val query = Query("Person").setFilter(composite)
        
        println(query)
        
        val johnRetrieved = ds.prepare(query).asSingleEntity()
        
        assertThat(johnRetrieved.properties["fname"] == "John", Is(true))
        
    }

    @Test
    fun sortListOfPeople(){

        val ds = DatastoreServiceFactory.getDatastoreService()

        val ann = Entity("Person")
        ann.setProperty("fname", "Ann")
        ann.setProperty("lname", "Doe")
        ds.put(ann)
        val cindy = Entity("Person")
        cindy.setProperty("fname", "Cindy")
        cindy.setProperty("lname", "Doe")
        ds.put(cindy)
        val beth = Entity("Person")
        beth.setProperty("fname", "Beth")
        beth.setProperty("lname", "Doe")
        ds.put(beth)
        val derek = Entity("Person")
        derek.setProperty("fname", "Derek")
        derek.setProperty("lname", "Doe")
        ds.put(derek)

        assertThat(ds.prepare(Query("Person")).countEntities(withLimit(10)), Is(4) )

        val query = Query("Person").addSort("fname",
                Query.SortDirection.ASCENDING)
        
        val personList = ds.prepare(query).asList(withLimit(20))

        assertThat(personList[0].properties["fname"] == "Ann", Is(true))
        assertThat(personList[1].properties["fname"] == "Beth", Is(true))
        assertThat(personList[2].properties["fname"] == "Cindy", Is(true))
        assertThat(personList[3].properties["fname"] == "Derek", Is(true))

    }


}
//
//  ComplexRelationTests.swift
//  GRDB
//
//  Created by Gwendal Roué on 25/05/2016.
//  Copyright © 2016 Gwendal Roué. All rights reserved.
//

import XCTest
import GRDB

private final class Person : RowConvertible, TableMapping {
    let id: Int64
    let name: String
    let birthCountryIsoCode: String?
    
    let birthCountry: Country?
    static let birthCountry = ForeignRelation(name: "birthCountry", tableName: "countries", foreignKey: ["birthCountryIsoCode": "isoCode"])
    
    let ruledCountry: Country?
    static let ruledCountry = ForeignRelation(name: "ruledCountry", tableName: "countries", foreignKey: ["id": "leaderID"])
    
    static func databaseTableName() -> String {
        return "persons"
    }
    
    init(_ row: Row) {
        id = row.value(named: "id")
        name = row.value(named: "name")
        birthCountryIsoCode = row.value(named: "birthCountryIsoCode")
        
        if let birthCountryRow = row.variant(named: Person.birthCountry.name) {
            birthCountry = Country(birthCountryRow)
        } else {
            birthCountry = nil
        }
        
        if let ruledCountryRow = row.variant(named: Person.ruledCountry.name) where ruledCountryRow.value(named: "isoCode") != nil {
            ruledCountry = Country(ruledCountryRow)
        } else {
            ruledCountry = nil
        }
    }
}

private final class Country: RowConvertible {
    let isoCode: String
    let name: String
    let leaderID: Int64?
    
    let leader: Person?
    static let leader = ForeignRelation(name: "leader", tableName: "persons", foreignKey: ["leaderID": "id"])
    
    init(_ row: Row) {
        isoCode = row.value(named: "isoCode")
        name = row.value(named: "name")
        leaderID = row.value(named: "leaderID")
        
        if let leaderRow = row.variant(named: Country.leader.name) {
            leader = Person(leaderRow)
        } else {
            leader = nil
        }
    }
}

class ComplexRelationTests: GRDBTestCase {
    func testDefaultRelationAlias() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
            }
            
            try dbQueue.inTransaction { db in
                try db.execute("INSERT INTO countries (isoCode, name) VALUES (?, ?)", arguments: ["FR", "France"])
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (NULL, ?, ?)", arguments: ["Arthur", "FR"])
                return .Commit
            }
            
            let request = Person
                .include(Person.birthCountry)
                .filter(sql: "\(Person.birthCountry.name).isoCode == 'FR'") // TODO1: pass "FR" as an argument
            
            XCTAssertEqual(
                sql(dbQueue, request),
                "SELECT \"persons\".*, \"birthCountry\".* " +
                "FROM \"persons\" " +
                "LEFT JOIN \"countries\" \"birthCountry\" ON \"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\" " +
                "WHERE birthCountry.isoCode == \'FR\'")
            
            dbQueue.inDatabase { db in
                let persons = request.fetchAll(db)
                XCTAssertEqual(persons.count, 1)
                
                XCTAssertEqual(persons[0].name, "Arthur")
                XCTAssertEqual(persons[0].birthCountry!.name, "France")
            }
        }
    }
    
    func testExplicitRelationAlias() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
            }
            
            try dbQueue.inTransaction { db in
                try db.execute("INSERT INTO countries (isoCode, name) VALUES (?, ?)", arguments: ["FR", "France"])
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (NULL, ?, ?)", arguments: ["Arthur", "FR"])
                return .Commit
            }
            
            let request = Person
                .include(Person.birthCountry.aliased("foo"))
                .filter(sql: "foo.isoCode == 'FR'") // TODO1: pass "FR" as an argument
                                                    // TODO2: make .filter(SQLColumn("foo.isoCode") == "FR") possible. Today it fails.
            XCTAssertEqual(
                sql(dbQueue, request),
                "SELECT \"persons\".*, \"foo\".* " +
                "FROM \"persons\" " +
                "LEFT JOIN \"countries\" \"foo\" ON \"foo\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\" " +
                "WHERE foo.isoCode == \'FR\'")
            
            dbQueue.inDatabase { db in
                let persons = request.fetchAll(db)
                XCTAssertEqual(persons.count, 1)
                
                XCTAssertEqual(persons[0].name, "Arthur")
                XCTAssertEqual(persons[0].birthCountry!.name, "France")
            }

            dbQueue.inDatabase { db in
                let request = Person
                    .include(Person.birthCountry.aliased("foo"))
                    .filter(sql: "foo.isoCode == 'US'") // TODO: pass "US" as an argument
                let persons = request.fetchAll(db)
                
                XCTAssertEqual(persons.count, 0)
            }
        }
    }
    
    func testPersonToRuledCountryAndToBirthCountry() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, leaderID INTEGER REFERENCES persons(id))")
            }
            
            try dbQueue.inTransaction { db in
                try db.execute("PRAGMA defer_foreign_keys = ON")
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [1, "Arthur", "FR"])
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [2, "Barbara", "FR"])
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [3, "John", "US"])
                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["FR", "France", 2])
                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["US", "United States", 3])
                return .Commit
            }
            
            let request = Person
                .include(Person.ruledCountry)
                .include(Person.birthCountry)
            
            XCTAssertEqual(
                sql(dbQueue, request),
                "SELECT \"persons\".*, \"ruledCountry\".*, \"birthCountry\".* " +
                "FROM \"persons\" " +
                "LEFT JOIN \"countries\" \"ruledCountry\" ON \"ruledCountry\".\"leaderID\" = \"persons\".\"id\" " +
                "LEFT JOIN \"countries\" \"birthCountry\" ON \"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\"")
            
            dbQueue.inDatabase { db in
                // TODO: sort persons using SQL
                let persons = request.fetchAll(db).sort { $0.id < $1.id }
                
                XCTAssertEqual(persons.count, 3)
                
                XCTAssertEqual(persons[0].name, "Arthur")
                XCTAssertNil(persons[0].ruledCountry)
                XCTAssertEqual(persons[0].birthCountry!.name, "France")
                
                XCTAssertEqual(persons[1].name, "Barbara")
                XCTAssertEqual(persons[1].ruledCountry!.name, "France")
                XCTAssertEqual(persons[1].birthCountry!.name, "France")
                
                XCTAssertEqual(persons[2].name, "John")
                XCTAssertEqual(persons[2].ruledCountry!.name, "United States")
                XCTAssertEqual(persons[2].birthCountry!.name, "United States")
            }
        }
    }
    
    func testPersonToRuledCountryAndToBirthCountryToLeaderToRuledCountry() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, leaderID INTEGER REFERENCES persons(id))")
            }
            
            try dbQueue.inTransaction { db in
                try db.execute("PRAGMA defer_foreign_keys = ON")
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [1, "Arthur", "FR"])
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [2, "Barbara", "FR"])
                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [3, "John", "US"])
                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["FR", "France", 2])
                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["US", "United States", 3])
                return .Commit
            }
            
            let request = Person
                .include(Person.ruledCountry
                    .include(Country.leader))
                .include(Person.birthCountry
                    .include(Country.leader
                        .include(Person.ruledCountry)))
            
            XCTAssertEqual(
                sql(dbQueue, request),
                "SELECT \"persons\".*, \"ruledCountry0\".*, \"leader0\".*, \"birthCountry\".*, \"leader1\".*, \"ruledCountry1\".* " +
                "FROM \"persons\" " +
                "LEFT JOIN \"countries\" \"ruledCountry0\" ON \"ruledCountry0\".\"leaderID\" = \"persons\".\"id\" " +
                "LEFT JOIN \"persons\" \"leader0\" ON \"leader0\".\"id\" = \"ruledCountry0\".\"leaderID\" " +
                "LEFT JOIN \"countries\" \"birthCountry\" ON \"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\" " +
                "LEFT JOIN \"persons\" \"leader1\" ON \"leader1\".\"id\" = \"birthCountry\".\"leaderID\" " +
                "LEFT JOIN \"countries\" \"ruledCountry1\" ON \"ruledCountry1\".\"leaderID\" = \"leader1\".\"id\"")
            
            dbQueue.inDatabase { db in
                // TODO: sort persons using SQL
                let persons = request.fetchAll(db).sort { $0.id < $1.id }
                
                XCTAssertEqual(persons.count, 3)
                
                XCTAssertEqual(persons[0].name, "Arthur")
                XCTAssertNil(persons[0].ruledCountry)
                XCTAssertEqual(persons[0].birthCountry!.name, "France")
                XCTAssertEqual(persons[0].birthCountry!.leader!.name, "Barbara")
                XCTAssertEqual(persons[0].birthCountry!.leader!.ruledCountry!.name, "France")
                
                XCTAssertEqual(persons[1].name, "Barbara")
                XCTAssertEqual(persons[1].ruledCountry!.name, "France")
                XCTAssertEqual(persons[1].ruledCountry!.leader!.name, "Barbara")
                XCTAssertEqual(persons[1].birthCountry!.name, "France")
                XCTAssertEqual(persons[1].birthCountry!.leader!.name, "Barbara")
                XCTAssertEqual(persons[1].birthCountry!.leader!.ruledCountry!.name, "France")
                
                XCTAssertEqual(persons[2].name, "John")
                XCTAssertEqual(persons[2].ruledCountry!.name, "United States")
                XCTAssertEqual(persons[2].ruledCountry!.leader!.name, "John")
                XCTAssertEqual(persons[2].birthCountry!.name, "United States")
                XCTAssertEqual(persons[2].birthCountry!.leader!.name, "John")
                XCTAssertEqual(persons[2].birthCountry!.leader!.ruledCountry!.name, "United States")
            }
        }
    }
}

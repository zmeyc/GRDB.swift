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
    static let birthCountry = ForeignRelation(variantName: "birthCountry", tableName: "countries", foreignKey: ["birthCountryIsoCode": "isoCode"])
    
    let ruledCountry: Country?
    static let ruledCountry = ForeignRelation(variantName: "ruledCountry", tableName: "countries", foreignKey: ["id": "leaderID"])
    
    static func databaseTableName() -> String {
        return "persons"
    }
    
    init(_ row: Row) {
        id = row.value(named: "id")
        name = row.value(named: "name")
        birthCountryIsoCode = row.value(named: "birthCountryIsoCode")
        
        if let birthCountryRow = row.variant(named: Person.birthCountry.variantName) {
            birthCountry = Country(birthCountryRow)
        } else {
            birthCountry = nil
        }
        
        if let ruledCountryRow = row.variant(named: Person.ruledCountry.variantName) where ruledCountryRow.value(named: "isoCode") != nil {
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
    static let leader = ForeignRelation(variantName: "leader", tableName: "persons", foreignKey: ["leaderID": "id"])
    
    init(_ row: Row) {
        isoCode = row.value(named: "isoCode")
        name = row.value(named: "name")
        leaderID = row.value(named: "leaderID")
        
        if let leaderRow = row.variant(named: Country.leader.variantName) {
            leader = Person(leaderRow)
        } else {
            leader = nil
        }
    }
}

class ComplexRelationTests: GRDBTestCase {
    func testAvailableVariants() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, bID REFERENCES b(id))")
                try db.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, cID REFERENCES c(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                try db.execute("INSERT INTO c (id, bID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                try db.execute("INSERT INTO d (id, cID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                
                let aTable = QueryInterfaceRequest<Void>(tableName : "a")
                let b = ForeignRelation(tableName: "b", foreignKey: ["id": "aID"])
                let c = ForeignRelation(tableName: "c", foreignKey: ["id": "bID"])
                let d = ForeignRelation(tableName: "d", foreignKey: ["id": "cID"])
                
                do {
                    let request = aTable.join(b)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                }
                
                do {
                    let request = aTable.include(b)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.join(b.join(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                }
                
                do {
                    let request = aTable.include(b.join(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.join(b.include(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertTrue(row.variant(named: "b")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(b.include(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                }
                
                do {
                    let request = aTable.join(b.join(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                }
                
                do {
                    let request = aTable.include(b.join(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") == nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.join(b.include(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertTrue(row.variant(named: "b")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(b.include(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") == nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                }
                
                do {
                    let request = aTable.join(b.join(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"d\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") != nil)
                    
                    XCTAssertTrue(row.variant(named: "b")!.isEmpty)
                    XCTAssertTrue(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.variant(named: "d")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(b.join(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"d\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") != nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                    XCTAssertTrue(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.variant(named: "d")!.isEmpty)
                }
                
                do {
                    let request = aTable.join(b.include(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c\".*, \"d\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") != nil)
                    
                    XCTAssertTrue(row.variant(named: "b")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.variant(named: "d")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(b.include(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".*, \"d\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\" " +
                        "LEFT JOIN \"c\" ON \"c\".\"bID\" = \"b\".\"id\" " +
                        "LEFT JOIN \"d\" ON \"d\".\"cID\" = \"c\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c") != nil)
                    XCTAssertTrue(row.variant(named: "b")?.variant(named: "c")?.variant(named: "d") != nil)
                    
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.isEmpty)
                    XCTAssertFalse(row.variant(named: "b")!.variant(named: "c")!.variant(named: "d")!.isEmpty)
                }
            }
        }
    }
    
    func testRelationVariantNameAndAlias() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                
                let aTable = QueryInterfaceRequest<Void>(tableName : "a")
                let bRelationUnnamed = ForeignRelation(tableName: "b", foreignKey: ["id": "aID"])
                let bRelationNamedAsTable = ForeignRelation(variantName: "b", tableName: "b", foreignKey: ["id": "aID"])
                let bRelationNamed = ForeignRelation(variantName: "bVariant", tableName: "b", foreignKey: ["id": "aID"])
                
                do {
                    let request = aTable.include(bRelationUnnamed)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(bRelationUnnamed.aliased("bAlias"))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bAlias\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bAlias\" ON \"bAlias\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(bRelationNamedAsTable)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(bRelationNamedAsTable.aliased("bAlias"))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bAlias\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bAlias\" ON \"bAlias\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "b") != nil)
                    XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(bRelationNamed)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bVariant\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bVariant\" ON \"bVariant\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "bVariant") != nil)
                    XCTAssertFalse(row.variant(named: "bVariant")!.isEmpty)
                }
                
                do {
                    let request = aTable.include(bRelationNamed.aliased("bAlias"))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bAlias\".* " +
                        "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bAlias\" ON \"bAlias\".\"aID\" = \"a\".\"id\"")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.variant(named: "bVariant") != nil)
                    XCTAssertFalse(row.variant(named: "bVariant")!.isEmpty)
                }
            }
        }
    }
    
    func testRelationAliasingOnSourceConflict() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inTransaction { db in
                try db.execute("PRAGMA defer_foreign_keys = ON")
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, bID REFERENCES b(id))")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("INSERT INTO a (id, bID) VALUES (?, ?)", arguments: [1, 1])
                try db.execute("INSERT INTO b (id, aID) VALUES (?, ?)", arguments: [1, 1])
                return .Commit
            }
            
            let aTable = QueryInterfaceRequest<Void>(tableName : "a")
            let bRelation = ForeignRelation(tableName: "b", foreignKey: ["id": "aID"])
            let aRelation = ForeignRelation(tableName: "a", foreignKey: ["id": "bID"])
            
            dbQueue.inDatabase { db in
                let request = aTable.include(bRelation.include(aRelation))
                XCTAssertEqual(
                    self.sql(db, request),
                    "SELECT \"a0\".*, \"b\".*, \"a1\".* " +
                    "FROM \"a\" \"a0\" " +
                    "LEFT JOIN \"b\" ON \"b\".\"aID\" = \"a0\".\"id\" " +
                    "LEFT JOIN \"a\" \"a1\" ON \"a1\".\"bID\" = \"b\".\"id\"")
                
                let row = Row.fetchOne(db, request)!
                XCTAssertTrue(row.variant(named: "b") != nil)
                XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                XCTAssertTrue(row.variant(named: "b")!.variant(named: "a") != nil)
                XCTAssertFalse(row.variant(named: "b")!.variant(named: "a")!.isEmpty)
            }
            
            dbQueue.inDatabase { db in
                let request = aTable.include(bRelation.include(aRelation.include(bRelation)))
                XCTAssertEqual(
                    self.sql(db, request),
                    "SELECT \"a0\".*, \"b0\".*, \"a1\".*, \"b1\".* " +
                    "FROM \"a\" \"a0\" " +
                    "LEFT JOIN \"b\" \"b0\" ON \"b0\".\"aID\" = \"a0\".\"id\" " +
                    "LEFT JOIN \"a\" \"a1\" ON \"a1\".\"bID\" = \"b\".\"id\" " +
                    "LEFT JOIN \"b\" \"b1\" ON \"b1\".\"aID\" = \"a1\".\"id\"")
                
                let row = Row.fetchOne(db, request)!
                XCTAssertTrue(row.variant(named: "b") != nil)
                XCTAssertFalse(row.variant(named: "b")!.isEmpty)
                XCTAssertTrue(row.variant(named: "b")!.variant(named: "a") != nil)
                XCTAssertFalse(row.variant(named: "b")!.variant(named: "a")!.isEmpty)
                XCTAssertTrue(row.variant(named: "b")!.variant(named: "a")!.variant(named: "b") != nil)
                XCTAssertFalse(row.variant(named: "b")!.variant(named: "a")!.variant(named: "b")!.isEmpty)
            }
        }
    }
    
    func testDefaultRelationAliasWithInclude() {
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
                .filter(sql: "\(Person.birthCountry.variantName).isoCode == 'FR'") // TODO1: pass "FR" as an argument
            
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
    
    func testDefaultRelationAliasWithJoin() {
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
                .join(Person.birthCountry)
                .filter(sql: "\(Person.birthCountry.variantName).isoCode == 'FR'") // TODO1: pass "FR" as an argument
            
            XCTAssertEqual(
                sql(dbQueue, request),
                "SELECT \"persons\".* " +
                "FROM \"persons\" " +
                "LEFT JOIN \"countries\" \"birthCountry\" ON \"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\" " +
                "WHERE birthCountry.isoCode == \'FR\'")
            
            dbQueue.inDatabase { db in
                let persons = request.fetchAll(db)
                XCTAssertEqual(persons.count, 1)
                
                XCTAssertEqual(persons[0].name, "Arthur")
                XCTAssertTrue(persons[0].birthCountry == nil)
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

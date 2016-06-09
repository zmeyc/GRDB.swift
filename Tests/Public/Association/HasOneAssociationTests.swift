import XCTest
#if SQLITE_HAS_CODEC
    import GRDBCipher
#else
    import GRDB
#endif

class HasOneAssociationTests: GRDBTestCase {
    
    func testJoin() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE owner (id INTEGER PRIMARY KEY, name TEXT)")
                try db.execute("CREATE TABLE owned (id INTEGER PRIMARY KEY, ownerID REFERENCES owner(id), name TEXT)")
                try db.execute("INSERT INTO owner (id, name) VALUES (1, 'owner1')")
                try db.execute("INSERT INTO owner (id, name) VALUES (2, 'owner2')")
                try db.execute("INSERT INTO owned (id, ownerID, name) VALUES (100, 1, 'owned1')")
            }
            let rootTable = QueryInterfaceRequest<Void>(tableName: "owner")
            let association = OneToOneAssociation(name: "owned", tableName: "owned", foreignKey: ["id": "ownerID"])
            let request = rootTable.include(association)
            XCTAssertEqual(sql(dbQueue, request), "SELECT \"owner\".*, \"owned\".* FROM \"owner\" LEFT JOIN \"owned\" ON \"owned\".\"ownerID\" = \"owner\".\"id\"")
            
            let rows = dbQueue.inDatabase { db in
                Row.fetchAll(db, request)
            }
            XCTAssertEqual(rows.count, 2)
            
            do {
                let row = rows[0]
                let rowPairs: [(String, DatabaseValueConvertible?)] = [("id", 1), ("name", "owner1"), ("id", 100), ("ownerID", 1), ("name", "owned1")]
                XCTAssertEqual(Array(row.columnNames), rowPairs.map { $0.0 })
                XCTAssertEqual(Array(row.databaseValues), rowPairs.map { $1?.databaseValue ?? .Null })
                
                let variant = row.variant(named: association.name)!
                let variantPairs: [(String, DatabaseValueConvertible?)] = [("id", 100), ("ownerID", 1), ("name", "owned1")]
                XCTAssertEqual(Array(variant.columnNames), variantPairs.map { $0.0 })
                XCTAssertEqual(Array(variant.databaseValues), variantPairs.map { $1?.databaseValue ?? .Null })
            }
            
            do {
                let row = rows[1]
                let rowPairs: [(String, DatabaseValueConvertible?)] = [("id", 2), ("name", "owner2"), ("id", nil), ("ownerID", nil), ("name", nil)]
                XCTAssertEqual(Array(row.columnNames), rowPairs.map { $0.0 })
                XCTAssertEqual(Array(row.databaseValues), rowPairs.map { $1?.databaseValue ?? .Null })
                
                let variant = row.variant(named: association.name)!
                let variantPairs: [(String, DatabaseValueConvertible?)] = [("id", nil), ("ownerID", nil), ("name", nil)]
                XCTAssertEqual(Array(variant.columnNames), variantPairs.map { $0.0 })
                XCTAssertEqual(Array(variant.databaseValues), variantPairs.map { $1?.databaseValue ?? .Null })
            }
        }
    }
    
    func testRecursiveJoin() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, friendID INTEGER REFERENCES persons(id))")
                try db.execute("INSERT INTO persons (id, name, friendID) VALUES (1, 'Arthur', NULL)")
                try db.execute("INSERT INTO persons (id, name, friendID) VALUES (2, 'Barbara', 1)")
            }
            let rootTable = QueryInterfaceRequest<Void>(tableName: "persons")
            let association = OneToOneAssociation(name: "friend", tableName: "persons", foreignKey: ["id": "friendID"])
            let request = rootTable.include(association)
            XCTAssertEqual(sql(dbQueue, request), "SELECT \"persons\".*, \"friend\".* FROM \"persons\" LEFT JOIN \"persons\" \"friend\" ON \"friend\".\"friendID\" = \"persons\".\"id\"")
            
            let rows = dbQueue.inDatabase { db in
                Row.fetchAll(db, request)
            }
            XCTAssertEqual(rows.count, 2)
            
            do {
                let row = rows[0]
                let rowPairs: [(String, DatabaseValueConvertible?)] = [("id", 1), ("name", "Arthur"), ("friendID", nil), ("id", 2), ("name", "Barbara"), ("friendID", 1)]
                XCTAssertEqual(Array(row.columnNames), rowPairs.map { $0.0 })
                XCTAssertEqual(Array(row.databaseValues), rowPairs.map { $1?.databaseValue ?? .Null })
                
                let variant = row.variant(named: association.name)!
                let variantPairs: [(String, DatabaseValueConvertible?)] = [("id", 2), ("name", "Barbara"), ("friendID", 1)]
                XCTAssertEqual(Array(variant.columnNames), variantPairs.map { $0.0 })
                XCTAssertEqual(Array(variant.databaseValues), variantPairs.map { $1?.databaseValue ?? .Null })
            }
            
            do {
                let row = rows[1]
                let rowPairs: [(String, DatabaseValueConvertible?)] = [("id", 2), ("name", "Barbara"), ("friendID", 1), ("id", nil), ("name", nil), ("friendID", nil)]
                XCTAssertEqual(Array(row.columnNames), rowPairs.map { $0.0 })
                XCTAssertEqual(Array(row.databaseValues), rowPairs.map { $1?.databaseValue ?? .Null })
                
                let variant = row.variant(named: association.name)!
                let variantPairs: [(String, DatabaseValueConvertible?)] = [("id", nil), ("name", nil), ("friendID", nil)]
                XCTAssertEqual(Array(variant.columnNames), variantPairs.map { $0.0 })
                XCTAssertEqual(Array(variant.databaseValues), variantPairs.map { $1?.databaseValue ?? .Null })
            }
        }
    }
    
    func testNestedRecursiveJoin() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, friendID INTEGER REFERENCES persons(id))")
                try db.execute("INSERT INTO persons (id, name, friendID) VALUES (1, 'Arthur', NULL)")
                try db.execute("INSERT INTO persons (id, name, friendID) VALUES (2, 'Barbara', 1)")
                try db.execute("INSERT INTO persons (id, name, friendID) VALUES (3, 'Craig', 2)")
            }
            let rootTable = QueryInterfaceRequest<Void>(tableName: "persons")
            let association = OneToOneAssociation(name: "friend", tableName: "persons", foreignKey: ["id": "friendID"])
            let request = rootTable.include(association.include(association))
            XCTAssertEqual(sql(dbQueue, request), "SELECT \"persons\".*, \"friend0\".*, \"friend1\".* FROM \"persons\" LEFT JOIN \"persons\" \"friend0\" ON \"friend0\".\"friendID\" = \"persons\".\"id\" LEFT JOIN \"persons\" \"friend1\" ON \"friend1\".\"friendID\" = \"friend0\".\"id\"")
            
            let rows = dbQueue.inDatabase { db in
                Row.fetchAll(db, request)
            }
            XCTAssertEqual(rows.count, 3)
            
            do {
                let row = rows[0]
                let rowPairs: [(String, DatabaseValueConvertible?)] = [("id", 1), ("name", "Arthur"), ("friendID", nil), ("id", 2), ("name", "Barbara"), ("friendID", 1), ("id", 3), ("name", "Craig"), ("friendID", 2)]
                XCTAssertEqual(Array(row.columnNames), rowPairs.map { $0.0 })
                XCTAssertEqual(Array(row.databaseValues), rowPairs.map { $1?.databaseValue ?? .Null })
                
                let variant = row.variant(named: association.name)!
                let variantPairs: [(String, DatabaseValueConvertible?)] = [("id", 2), ("name", "Barbara"), ("friendID", 1), ("id", 3), ("name", "Craig"), ("friendID", 2)]
                XCTAssertEqual(Array(variant.columnNames), variantPairs.map { $0.0 })
                XCTAssertEqual(Array(variant.databaseValues), variantPairs.map { $1?.databaseValue ?? .Null })
                
                let variant2 = variant.variant(named: association.name)!
                let variant2Pairs: [(String, DatabaseValueConvertible?)] = [("id", 3), ("name", "Craig"), ("friendID", 2)]
                XCTAssertEqual(Array(variant2.columnNames), variant2Pairs.map { $0.0 })
                XCTAssertEqual(Array(variant2.databaseValues), variant2Pairs.map { $1?.databaseValue ?? .Null })
            }
        }
    }
}

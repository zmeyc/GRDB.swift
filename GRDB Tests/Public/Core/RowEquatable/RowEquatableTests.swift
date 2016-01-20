import XCTest
import GRDB

struct RowEquatablePerson : RowEquatable {
    let id: Int64?
    func hasEqualPrimaryKey(other: RowEquatablePerson) -> Bool {
        return id != nil && id == other.id
    }
}

struct EquatablePerson : Equatable, _RowEquatable {
    let id: Int64
}

func ==(lhs: EquatablePerson, rhs: EquatablePerson) -> Bool {
    return lhs.id == rhs.id
}

struct MutableDatabasePersistablePerson : MutableDatabasePersistable, _RowEquatable {
    let id: Int64?
    
    static func databaseTableName() -> String { return "persons" }
    var persistentDictionary: [String: DatabaseValueConvertible?] {
        return ["id": id]
    }
}

class PersonRecord: Record {
    var id: Int64?
    init(id: Int64?) {
        self.id = id
        super.init()
    }
    override static func databaseTableName() -> String { return "persons" }
    override var persistentDictionary: [String: DatabaseValueConvertible?] {
        return ["id": id]
    }
    required init(_ row: Row) {
        super.init(row)
    }
}

func containsUsingHasEqualMethod<T: _RowEquatable>(db: Database, haystack: [T], needle: T) -> Bool {
    return haystack.contains { element in
        return element.hasEqualPrimaryKey(needle, inDatabase: db)
    }
}

func containsUsingComparator<T: _RowEquatable where T == T.Comparator.Compared>(db: Database, haystack: [T], needle: T) -> Bool {
    let comparator = T.comparator(db)
    return haystack.contains { element in
        return comparator.equalPrimaryKey(element, needle)
    }
}

class RowEquatableTests: GRDBTestCase {

    override func setUp() {
        super.setUp()
        try! dbQueue.inDatabase { db in
            try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY)")
        }
    }

    func testRowEquatablePerson() {
        let haystack = [RowEquatablePerson(id: 1), RowEquatablePerson(id: 2)]
        let needle1 = RowEquatablePerson(id: 1)
        let needle2 = RowEquatablePerson(id: 2)
        let needle3 = RowEquatablePerson(id: 3)
        dbQueue.inDatabase { db in
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle3))
            
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingComparator(db, haystack: haystack, needle: needle3))
        }
    }
    
    func testEquatablePerson() {
        let haystack = [EquatablePerson(id: 1), EquatablePerson(id: 2)]
        let needle1 = EquatablePerson(id: 1)
        let needle2 = EquatablePerson(id: 2)
        let needle3 = EquatablePerson(id: 3)
        dbQueue.inDatabase { db in
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle3))
            
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingComparator(db, haystack: haystack, needle: needle3))
        }
    }
    
    func testMutableDatabasePersistablePerson() {
        let haystack = [MutableDatabasePersistablePerson(id: 1), MutableDatabasePersistablePerson(id: 2)]
        let needle1 = MutableDatabasePersistablePerson(id: 1)
        let needle2 = MutableDatabasePersistablePerson(id: 2)
        let needle3 = MutableDatabasePersistablePerson(id: 3)
        dbQueue.inDatabase { db in
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle3))
            
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingComparator(db, haystack: haystack, needle: needle3))
        }
    }
    
    func testPersonRecord() {
        let haystack = [PersonRecord(id: 1), PersonRecord(id: 2)]
        let needle1 = PersonRecord(id: 1)
        let needle2 = PersonRecord(id: 2)
        let needle3 = PersonRecord(id: 3)
        dbQueue.inDatabase { db in
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingHasEqualMethod(db, haystack: haystack, needle: needle3))
            
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle1))
            XCTAssertTrue(containsUsingComparator(db, haystack: haystack, needle: needle2))
            XCTAssertFalse(containsUsingComparator(db, haystack: haystack, needle: needle3))
        }
    }
}

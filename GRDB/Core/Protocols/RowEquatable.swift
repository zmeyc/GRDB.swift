/// The general protocol for objects that can be compared by primary key.
///
/// In order to adopt it, you have a few options:
///
/// - Implement its only required functions:
///
///     struct RowEquatablePerson : RowEquatable {
///         let id: Int64?
///         func hasEqualPrimaryKey(other: RowEquatablePerson, inDatabase db: Database) -> Bool {
///             return id != nil && id == other.id
///         }
///     }
///
/// - Subclass Record.
///
///     class RecordPerson : Record { ... }
///
/// - Adopt Equatable, and declare RowEquatable adoption.
///
///     struct EquatablePerson : Equatable, RowEquatable {
///         let id: Int64
///     }
///
///     func ==(lhs: EquatablePerson, rhs: EquatablePerson) -> Bool {
///         return lhs.id == rhs.id
///     }
///
/// - Adopt MutableDatabasePersistable, and declare RowEquatable adoption.
///
///     struct PersistablePerson : MutableDatabasePersistable, RowEquatable {
///         let id: Int64?
///
///         static func databaseTableName() -> String { return "persons" }
///         var persistentDictionary: [String: DatabaseValueConvertible?] {
///             return ["id": id]
///         }
///     }
public protocol RowEquatable {
    /// Optional static method that returns a function that compares two
    /// objects. Explicit implementations can provide an optimization
    /// opportunity.
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool
    
    /// Compares the primary keys of *self* and *other*.
    ///
    /// The result must be false if either primary key is null.
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool
}

extension RowEquatable {
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool {
        return { $0.hasEqualPrimaryKey($1, inDatabase: db) }
    }
}


/// Free implementation of RowEquatable for MutableDatabasePersistable
///
///     struct Person : MutableDatabasePersistable, RowEquatable {
///         let id: Int64?
///
///         static func databaseTableName() -> String { return "persons" }
///         var persistentDictionary: [String: DatabaseValueConvertible?] {
///             return ["id": id]
///         }
///     }
public extension MutableDatabasePersistable where Self: RowEquatable {
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool {
        let tableName = databaseTableName()
        let primaryKey = db.primaryKey(tableName)
        let columns = primaryKey.columns
        return { (o1, o2) -> Bool in
            let values1 = databaseValues(forColumns: columns, inDictionary: o1.persistentDictionary)
            guard values1.indexOf({ !$0.isNull }) != nil else { return false }
            let values2 = databaseValues(forColumns: columns, inDictionary: o2.persistentDictionary)
            guard values2.indexOf({ !$0.isNull }) != nil else { return false }
            return values1 == values2
        }
    }
    
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool {
        return self.dynamicType.primaryKeyComparator(db)(self, other)
    }
}

/// Free implementation of RowEquatable for Equatable
///
///     struct EquatablePerson : Equatable, RowEquatable {
///         let id: Int64
///     }
///
///     func ==(lhs: EquatablePerson, rhs: EquatablePerson) -> Bool {
///         return lhs.id == rhs.id
///     }
public extension Equatable where Self: RowEquatable {
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool {
        return { $0 == $1 }
    }
    
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool {
        return self == other
    }
}


struct RowEquatablePerson : RowEquatable {
    let id: Int64?
    func hasEqualPrimaryKey(other: RowEquatablePerson, inDatabase db: Database) -> Bool {
        return id != nil && id == other.id
    }
}


struct EquatablePerson : Equatable, RowEquatable {
    let id: Int64
}

func ==(lhs: EquatablePerson, rhs: EquatablePerson) -> Bool {
    return lhs.id == rhs.id
}

struct PersistablePerson : MutableDatabasePersistable, RowEquatable {
    let id: Int64?

    static func databaseTableName() -> String { return "persons" }
    var persistentDictionary: [String: DatabaseValueConvertible?] {
        return ["id": id]
    }
}


class C: Equatable, RowEquatable {
}

func ==(lhs: C, rhs: C) -> Bool {
    return false
}

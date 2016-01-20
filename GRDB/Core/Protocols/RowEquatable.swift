// MARK: - _RowEquatable
public protocol _RowComparator {
    typealias Compared
    func equalPrimaryKey(lhs: Compared, _ rhs: Compared) -> Bool
}
public protocol _RowEquatable {
    typealias Comparator: _RowComparator
    static func comparator(db: Database) -> Comparator
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool
}

extension _RowEquatable where Self == Comparator.Compared {
    public func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool {
        return self.dynamicType.comparator(db).equalPrimaryKey(self, other)
    }
}


// MARK: - RowEquatable

public protocol RowEquatable: _RowEquatable {
    func hasEqualPrimaryKey(other: Self) -> Bool
}

public extension RowEquatable {
    static func comparator(db: Database) -> RowEquatableComparator<Self> {
        return RowEquatableComparator()
    }
}

public struct RowEquatableComparator<Compared: RowEquatable>: _RowComparator {
    public func equalPrimaryKey(lhs: Compared, _ rhs: Compared) -> Bool {
        return lhs.hasEqualPrimaryKey(rhs)
    }
}


// MARK: - MutableDatabasePersistable

public extension MutableDatabasePersistable where Self: _RowEquatable {
    typealias Compared = MutableDatabasePersistableComparator<Self>
    
    static func comparator(db: Database) -> MutableDatabasePersistableComparator<Self> {
        return MutableDatabasePersistableComparator(db: db, tableName: databaseTableName())
    }
}

public struct MutableDatabasePersistableComparator<Compared: MutableDatabasePersistable>: _RowComparator {
    let columns: [String]
    init(db: Database, tableName: String) {
        let primaryKey = db.primaryKey(tableName)
        columns = primaryKey.columns
    }
    public func equalPrimaryKey(lhs: Compared, _ rhs: Compared) -> Bool {
        let lValues = databaseValues(forColumns: columns, inDictionary: lhs.persistentDictionary)
        guard lValues.indexOf({ !$0.isNull }) != nil else { return false }
        let rValues = databaseValues(forColumns: columns, inDictionary: rhs.persistentDictionary)
        guard rValues.indexOf({ !$0.isNull }) != nil else { return false }
        return lValues == rValues
    }
}


// MARK: - Equatable

public extension Equatable where Self: _RowEquatable {
    static func comparator(db: Database) -> EquatableComparator<Self> {
        return EquatableComparator()
    }
}

public struct EquatableComparator<Compared: Equatable>: _RowComparator {
    public func equalPrimaryKey(lhs: Compared, _ rhs: Compared) -> Bool {
        return lhs == rhs
    }
}

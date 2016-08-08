// MARK: - PersistenceError

/// An error thrown by a type that adopts Persistable.
public enum PersistenceError: Error {
    
    /// Thrown by MutablePersistable.update() when no matching row could be
    /// found in the database.
    case recordNotFound(MutablePersistable)
}

extension PersistenceError : CustomStringConvertible {
    /// A textual representation of `self`.
    public var description: String {
        switch self {
        case .recordNotFound(let persistable):
            return "Not found: \(persistable)"
        }
    }
}

private func databaseValue(for column: String, inDictionary dictionary: [String: DatabaseValueConvertible?]) -> DatabaseValue {
    if let value = dictionary[column] {
        return value?.databaseValue ?? .null
    }
    let column = column.lowercased()
    for (key, value) in dictionary where key.lowercased() == column {
        return value?.databaseValue ?? .null
    }
    return .null
}

private func databaseValues(for columns: [String], inDictionary dictionary: [String: DatabaseValueConvertible?]) -> [DatabaseValue] {
    return columns.map { databaseValue(for: $0, inDictionary: dictionary) }
}


// MARK: - MutablePersistable

/// Types that adopt MutablePersistable can be inserted, updated, and deleted.
///
/// This protocol is intented for types that have an INTEGER PRIMARY KEY, and
/// are interested in the inserted RowID: they can mutate themselves upon
/// successful insertion with the didInsert(with:for:) method.
///
/// The insert() and save() methods are mutating methods.
public protocol MutablePersistable : TableMapping {
    
    /// Returns the values that should be stored in the database.
    ///
    /// Keys of the returned dictionary must match the column names of the
    /// target database table (see TableMapping.databaseTableName()).
    ///
    /// In particular, primary key columns, if any, must be included.
    ///
    ///     struct Person : MutablePersistable {
    ///         var id: Int64?
    ///         var name: String?
    ///
    ///         var persistentDictionary: [String: DatabaseValueConvertible?] {
    ///             return ["id": id, "name": name]
    ///         }
    ///     }
    var persistentDictionary: [String: DatabaseValueConvertible?] { get }
    
    /// Notifies the record that it was succesfully inserted.
    ///
    /// Do not call this method directly: it is called for you, in a protected
    /// dispatch queue, with the inserted RowID and the eventual
    /// INTEGER PRIMARY KEY column name.
    ///
    /// This method is optional: the default implementation does nothing.
    ///
    ///     struct Person : MutablePersistable {
    ///         var id: Int64?
    ///         var name: String?
    ///
    ///         mutating func didInsert(with rowID: Int64, for column: String?) {
    ///             self.id = rowID
    ///         }
    ///     }
    ///
    /// - parameters:
    ///     - rowID: The inserted rowID.
    ///     - column: The name of the eventual INTEGER PRIMARY KEY column.
    mutating func didInsert(with rowID: Int64, for column: String?)
    
    
    // MARK: - CRUD
    
    /// Executes an INSERT statement.
    ///
    /// This method is guaranteed to have inserted a row in the database if it
    /// returns without error.
    ///
    /// Upon successful insertion, the didInsert(with:for:) method
    /// is called with the inserted RowID and the eventual INTEGER PRIMARY KEY
    /// column name.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of insert(). In their implementation, it is recommended
    /// that they invoke the performInsert() method.
    ///
    /// - parameter db: A database connection.
    /// - throws: A DatabaseError whenever an SQLite error occurs.
    mutating func insert(_ db: Database) throws
    
    /// Executes an UPDATE statement.
    ///
    /// This method is guaranteed to have updated a row in the database if it
    /// returns without error.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of update(). In their implementation, it is recommended
    /// that they invoke the performUpdate() method.
    ///
    /// - parameter db: A database connection.
    /// - parameter columns: The columns to update.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    ///   PersistenceError.recordNotFound is thrown if the primary key does not
    ///   match any row in the database.
    func update(_ db: Database, columns: Set<String>) throws
    
    /// Executes an INSERT or an UPDATE statement so that `self` is saved in
    /// the database.
    ///
    /// If the receiver has a non-nil primary key and a matching row in the
    /// database, this method performs an update.
    ///
    /// Otherwise, performs an insert.
    ///
    /// This method is guaranteed to have inserted or updated a row in the
    /// database if it returns without error.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of save(). In their implementation, it is recommended
    /// that they invoke the performSave() method.
    ///
    /// - parameter db: A database connection.
    /// - throws: A DatabaseError whenever an SQLite error occurs, or errors
    ///   thrown by update().
    mutating func save(_ db: Database) throws
    
    /// Executes a DELETE statement.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of delete(). In their implementation, it is recommended
    /// that they invoke the performDelete() method.
    ///
    /// - parameter db: A database connection.
    /// - returns: Whether a database row was deleted.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    @discardableResult func delete(_ db: Database) throws -> Bool
    
    /// Returns true if and only if the primary key matches a row in
    /// the database.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of exists(). In their implementation, it is recommended
    /// that they invoke the performExists() method.
    ///
    /// - parameter db: A database connection.
    /// - returns: Whether the primary key matches a row in the database.
    func exists(_ db: Database) -> Bool
}

public extension MutablePersistable {
    
    /// Notifies the record that it was succesfully inserted.
    ///
    /// The default implementation does nothing.
    mutating func didInsert(with rowID: Int64, for column: String?) {
    }
    
    
    // MARK: - CRUD
    
    /// Executes an INSERT statement.
    ///
    /// The default implementation for insert() invokes performInsert().
    mutating func insert(_ db: Database) throws {
        try performInsert(db)
    }
    
    /// Executes an UPDATE statement.
    ///
    /// - parameter db: A database connection.
    /// - parameter columns: The columns to update.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    ///   PersistenceError.NotFound is thrown if the primary key does not
    ///   match any row in the database.
    func update(_ db: Database, columns: Set<String>) throws {
        try performUpdate(db, columns: columns)
    }
    
    /// Executes an UPDATE statement.
    ///
    /// - parameter db: A database connection.
    /// - parameter columns: The columns to update.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    ///   PersistenceError.NotFound is thrown if the primary key does not
    ///   match any row in the database.
    func update<Sequence: Swift.Sequence>(_ db: Database, columns: Sequence) throws where Sequence.Iterator.Element == Column {
        try update(db, columns: Set(columns.map { $0.name }))
    }
    
    /// Executes an UPDATE statement.
    ///
    /// - parameter db: A database connection.
    /// - parameter columns: The columns to update.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    ///   PersistenceError.NotFound is thrown if the primary key does not
    ///   match any row in the database.
    func update<Sequence: Swift.Sequence>(_ db: Database, columns: Sequence) throws where Sequence.Iterator.Element == String {
        try update(db, columns: Set(columns))
    }
    
    /// Executes an UPDATE statement that updates all table columns.
    ///
    /// - parameter db: A database connection.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    ///   PersistenceError.NotFound is thrown if the primary key does not
    ///   match any row in the database.
    func update(_ db: Database) throws {
        let databaseTableName = self.dynamicType.databaseTableName
        let columns = try db.columns(in: databaseTableName)
        try update(db, columns: Set(columns.map { $0.name }))
    }
    
    /// Executes an INSERT or an UPDATE statement so that `self` is saved in
    /// the database.
    ///
    /// The default implementation for save() invokes performSave().
    mutating func save(_ db: Database) throws {
        try performSave(db)
    }
    
    /// Executes a DELETE statement.
    ///
    /// The default implementation for delete() invokes performDelete().
    @discardableResult func delete(_ db: Database) throws -> Bool {
        return try performDelete(db)
    }
    
    /// Returns true if and only if the primary key matches a row in
    /// the database.
    ///
    /// The default implementation for exists() invokes performExists().
    func exists(_ db: Database) -> Bool {
        return performExists(db)
    }
    
    
    // MARK: - CRUD Internals
    
    private func canUpdate(_ db: Database) -> Bool {
        // Fail early if database table does not exist.
        let databaseTableName = self.dynamicType.databaseTableName
        guard let primaryKey = try! db.primaryKey(databaseTableName) else {
            return false
        }
        
        let persistentDictionary = self.persistentDictionary
        for column in primaryKey.columns where !databaseValue(for: column, inDictionary: persistentDictionary).isNull {
            return true
        }
        return false
    }
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt MutablePersistable.
    ///
    /// performInsert() provides the default implementation for insert(). Types
    /// that adopt MutablePersistable can invoke performInsert() in their
    /// implementation of insert(). They should not provide their own
    /// implementation of performInsert().
    mutating func performInsert(_ db: Database) throws {
        let dataMapper = DataMapper(db, self)
        try dataMapper.insertStatement().execute()
        didInsert(with: db.lastInsertedRowID, for: dataMapper.primaryKey?.rowIDColumn)
    }
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt MutablePersistable.
    ///
    /// performUpdate() provides the default implementation for update(). Types
    /// that adopt MutablePersistable can invoke performUpdate() in their
    /// implementation of update(). They should not provide their own
    /// implementation of performUpdate().
    ///
    /// - parameter db: A database connection.
    /// - parameter columns: The columns to update.
    /// - throws: A DatabaseError is thrown whenever an SQLite error occurs.
    ///   PersistenceError.NotFound is thrown if the primary key does not
    ///   match any row in the database.
    func performUpdate(_ db: Database, columns: Set<String>) throws {
        try DataMapper(db, self).updateStatement(columns: columns).execute()
        if db.changesCount == 0 {
            throw PersistenceError.recordNotFound(self)
        }
    }
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt MutablePersistable.
    ///
    /// performSave() provides the default implementation for save(). Types
    /// that adopt MutablePersistable can invoke performSave() in their
    /// implementation of save(). They should not provide their own
    /// implementation of performSave().
    ///
    /// This default implementation forwards the job to `update` or `insert`.
    mutating func performSave(_ db: Database) throws {
        // Make sure we call self.insert and self.update so that classes
        // that override insert or save have opportunity to perform their
        // custom job.
        
        if canUpdate(db) {
            do {
                try update(db)
            } catch PersistenceError.recordNotFound {
                // TODO: check that the not persisted objet is self
                //
                // Why? Adopting types could override update() and update
                // another object which may be the one throwing this error.
                try insert(db)
            }
        } else {
            try insert(db)
        }
    }
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt MutablePersistable.
    ///
    /// performDelete() provides the default implementation for deelte(). Types
    /// that adopt MutablePersistable can invoke performDelete() in
    /// their implementation of delete(). They should not provide their own
    /// implementation of performDelete().
    func performDelete(_ db: Database) throws -> Bool {
        try DataMapper(db, self).deleteStatement().execute()
        return db.changesCount > 0
    }
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt MutablePersistable.
    ///
    /// performExists() provides the default implementation for exists(). Types
    /// that adopt MutablePersistable can invoke performExists() in
    /// their implementation of exists(). They should not provide their own
    /// implementation of performExists().
    func performExists(_ db: Database) -> Bool {
        return (Row.fetchOne(DataMapper(db, self).existsStatement()) != nil)
    }
    
}


// MARK: - Persistable

/// Types that adopt Persistable can be inserted, updated, and deleted.
///
/// This protocol is intented for types that don't have an INTEGER PRIMARY KEY.
///
/// Unlike MutablePersistable, the insert() and save() methods are not
/// mutating methods.
public protocol Persistable : MutablePersistable {
    
    /// Notifies the record that it was succesfully inserted.
    ///
    /// Do not call this method directly: it is called for you, in a protected
    /// dispatch queue, with the inserted RowID and the eventual
    /// INTEGER PRIMARY KEY column name.
    ///
    /// This method is optional: the default implementation does nothing.
    ///
    /// If you need a mutating variant of this method, adopt the
    /// MutablePersistable protocol instead.
    ///
    /// - parameters:
    ///     - rowID: The inserted rowID.
    ///     - column: The name of the eventual INTEGER PRIMARY KEY column.
    func didInsert(with rowID: Int64, for column: String?)
    
    /// Executes an INSERT statement.
    ///
    /// This method is guaranteed to have inserted a row in the database if it
    /// returns without error.
    ///
    /// Upon successful insertion, the didInsert(with:for:) method
    /// is called with the inserted RowID and the eventual INTEGER PRIMARY KEY
    /// column name.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of insert(). In their implementation, it is recommended
    /// that they invoke the performInsert() method.
    ///
    /// - parameter db: A database connection.
    /// - throws: A DatabaseError whenever an SQLite error occurs.
    func insert(_ db: Database) throws
    
    /// Executes an INSERT or an UPDATE statement so that `self` is saved in
    /// the database.
    ///
    /// If the receiver has a non-nil primary key and a matching row in the
    /// database, this method performs an update.
    ///
    /// Otherwise, performs an insert.
    ///
    /// This method is guaranteed to have inserted or updated a row in the
    /// database if it returns without error.
    ///
    /// This method has a default implementation, so your adopting types don't
    /// have to implement it. Yet your types can provide their own
    /// implementation of save(). In their implementation, it is recommended
    /// that they invoke the performSave() method.
    ///
    /// - parameter db: A database connection.
    /// - throws: A DatabaseError whenever an SQLite error occurs, or errors
    ///   thrown by update().
    func save(_ db: Database) throws
}

public extension Persistable {
    
    /// Notifies the record that it was succesfully inserted.
    ///
    /// The default implementation does nothing.
    func didInsert(with rowID: Int64, for column: String?) {
    }
    
    // MARK: - Immutable CRUD
    
    /// Executes an INSERT statement.
    ///
    /// The default implementation for insert() invokes performInsert().
    func insert(_ db: Database) throws {
        try performInsert(db)
    }
    
    /// Executes an INSERT or an UPDATE statement so that `self` is saved in
    /// the database.
    ///
    /// The default implementation for save() invokes performSave().
    func save(_ db: Database) throws {
        try performSave(db)
    }
    
    
    // MARK: - Immutable CRUD Internals
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt Persistable.
    ///
    /// performInsert() provides the default implementation for insert(). Types
    /// that adopt Persistable can invoke performInsert() in their
    /// implementation of insert(). They should not provide their own
    /// implementation of performInsert().
    func performInsert(_ db: Database) throws {
        let dataMapper = DataMapper(db, self)
        try dataMapper.insertStatement().execute()
        didInsert(with: db.lastInsertedRowID, for: dataMapper.primaryKey?.rowIDColumn)
    }
    
    /// Don't invoke this method directly: it is an internal method for types
    /// that adopt Persistable.
    ///
    /// performSave() provides the default implementation for save(). Types
    /// that adopt Persistable can invoke performSave() in their
    /// implementation of save(). They should not provide their own
    /// implementation of performSave().
    ///
    /// This default implementation forwards the job to `update` or `insert`.
    func performSave(_ db: Database) throws {
        // Make sure we call self.insert and self.update so that classes that
        // override insert or save have opportunity to perform their custom job.
        
        if canUpdate(db) {
            do {
                try update(db)
            } catch PersistenceError.recordNotFound {
                // TODO: check that the not persisted objet is self
                //
                // Why? Adopting types could override update() and update another
                // object which may be the one throwing this error.
                try insert(db)
            }
        } else {
            try insert(db)
        }
    }
    
}


// MARK: - DataMapper

/// DataMapper takes care of Persistable CRUD
final class DataMapper {
    
    /// The database
    let db: Database
    
    /// The persistable
    let persistable: MutablePersistable
    
    /// DataMapper keeps a copy the persistable's persistentDictionary, so
    /// that this dictionary is built once whatever the database operation.
    /// It is guaranteed to have at least one (key, value) pair.
    let persistentDictionary: [String: DatabaseValueConvertible?]
    
    /// The table name
    let databaseTableName: String
    
    /// The table primary key
    let primaryKey: PrimaryKeyInfo?
    
    init(_ db: Database, _ persistable: MutablePersistable) {
        // Fail early if database table does not exist.
        let databaseTableName = persistable.dynamicType.databaseTableName
        let primaryKey = try! db.primaryKey(databaseTableName)
        
        // Fail early if persistentDictionary is empty
        let persistentDictionary = persistable.persistentDictionary
        GRDBPrecondition(persistentDictionary.count > 0, "\(persistable.dynamicType).persistentDictionary: invalid empty dictionary")
        
        self.db = db
        self.persistable = persistable
        self.persistentDictionary = persistentDictionary
        self.databaseTableName = databaseTableName
        self.primaryKey = primaryKey
    }
    
    func insertStatement() -> UpdateStatement {
        let query = InsertQuery(
            tableName: databaseTableName,
            insertedColumns: Array(persistentDictionary.keys))
        let statement = try! db.cachedUpdateStatement(query.sql)
        statement.unsafeSetArguments(StatementArguments(persistentDictionary.values))
        return statement
    }
    
    func updateStatement(columns: Set<String>) -> UpdateStatement {
        // Fail early if primary key does not resolve to a database row.
        let primaryKeyColumns = primaryKey?.columns ?? []
        let primaryKeyValues = databaseValues(for: primaryKeyColumns, inDictionary: persistentDictionary)
        GRDBPrecondition(primaryKeyValues.contains { !$0.isNull }, "record can not be identified. persistentDictionary must contain non-nil value(s) for the key(s) \(primaryKeyColumns.joined(separator: ","))")
        
        // Don't update primary key columns
        var updatedColumns = Array(persistentDictionary.keys)
            .map { $0.lowercased() }
            .filter { (lowercaseKey: String) in columns.contains { $0.lowercased() == lowercaseKey } }
            .filter { (lowercaseKey: String) in !primaryKeyColumns.contains { $0.lowercased() == lowercaseKey } }
        if updatedColumns.isEmpty {
            // IMPLEMENTATION NOTE
            //
            // It is important to update something, so that
            // TransactionObserver can observe a change even though this
            // change is useless.
            //
            // The goal is to be able to write tests with minimal tables,
            // including tables made of a single primary key column.
            updatedColumns = primaryKeyColumns
        }
        let updatedValues = databaseValues(for: updatedColumns, inDictionary: persistentDictionary)
        
        let query = UpdateQuery(
            tableName: databaseTableName,
            updatedColumns: updatedColumns,
            conditionColumns: primaryKeyColumns)
        let statement = try! db.cachedUpdateStatement(query.sql)
        statement.unsafeSetArguments(StatementArguments(updatedValues + primaryKeyValues))
        return statement
    }
    
    func deleteStatement() -> UpdateStatement {
        // Fail early if primary key does not resolve to a database row.
        let primaryKeyColumns = primaryKey?.columns ?? []
        let primaryKeyValues = databaseValues(for: primaryKeyColumns, inDictionary: persistentDictionary)
        GRDBPrecondition(primaryKeyValues.contains { !$0.isNull }, "record can not be identified. persistentDictionary must contain non-nil value(s) for the key(s) \(primaryKeyColumns.joined(separator: ","))")
        
        let query = DeleteQuery(
            tableName: databaseTableName,
            conditionColumns: primaryKeyColumns)
        let statement = try! db.cachedUpdateStatement(query.sql)
        statement.unsafeSetArguments(StatementArguments(primaryKeyValues))
        return statement
    }
    
    func existsStatement() -> SelectStatement {
        // Fail early if primary key does not resolve to a database row.
        let primaryKeyColumns = primaryKey?.columns ?? []
        let primaryKeyValues = databaseValues(for: primaryKeyColumns, inDictionary: persistentDictionary)
        GRDBPrecondition(primaryKeyValues.contains { !$0.isNull }, "record can not be identified. persistentDictionary must contain non-nil value(s) for the key(s) \(primaryKeyColumns.joined(separator: ","))")
        
        let query = ExistsQuery(
            tableName: databaseTableName,
            conditionColumns: primaryKeyColumns)
        let statement = try! db.cachedSelectStatement(query.sql)
        statement.unsafeSetArguments(StatementArguments(primaryKeyValues))
        return statement
    }
}


// MARK: - InsertQuery

private struct InsertQuery {
    let tableName: String
    let insertedColumns: [String]
}

extension InsertQuery : Hashable {
    var hashValue: Int { return tableName.hashValue }
}

private func == (lhs: InsertQuery, rhs: InsertQuery) -> Bool {
    if lhs.tableName != rhs.tableName { return false }
    return lhs.insertedColumns == rhs.insertedColumns
}

extension InsertQuery {
    static let sqlCache = ReadWriteBox([InsertQuery: String]())
    var sql: String {
        if let sql = InsertQuery.sqlCache.read({ $0[self] }) {
            return sql
        }
        let columnsSQL = insertedColumns.map { $0.quotedDatabaseIdentifier }.joined(separator: ",")
        let valuesSQL = databaseQuestionMarks(count: insertedColumns.count)
        let sql = "INSERT INTO \(tableName.quotedDatabaseIdentifier) (\(columnsSQL)) VALUES (\(valuesSQL))"
        InsertQuery.sqlCache.write { $0[self] = sql }
        return sql
    }
}


// MARK: - UpdateQuery

private struct UpdateQuery {
    let tableName: String
    let updatedColumns: [String]
    let conditionColumns: [String]
}

extension UpdateQuery : Hashable {
    var hashValue: Int { return tableName.hashValue }
}

private func == (lhs: UpdateQuery, rhs: UpdateQuery) -> Bool {
    if lhs.tableName != rhs.tableName { return false }
    if lhs.updatedColumns != rhs.updatedColumns { return false }
    return lhs.conditionColumns == rhs.conditionColumns
}

extension UpdateQuery {
    static let sqlCache = ReadWriteBox([UpdateQuery: String]())
    var sql: String {
        if let sql = UpdateQuery.sqlCache.read({ $0[self] }) {
            return sql
        }
        let updateSQL = updatedColumns.map { "\($0.quotedDatabaseIdentifier)=?" }.joined(separator: ",")
        let whereSQL = conditionColumns.map { "\($0.quotedDatabaseIdentifier)=?" }.joined(separator: " AND ")
        let sql = "UPDATE \(tableName.quotedDatabaseIdentifier) SET \(updateSQL) WHERE \(whereSQL)"
        UpdateQuery.sqlCache.write { $0[self] = sql }
        return sql
    }
}


// MARK: - DeleteQuery

private struct DeleteQuery {
    let tableName: String
    let conditionColumns: [String]
}

extension DeleteQuery {
    var sql: String {
        let whereSQL = conditionColumns.map { "\($0.quotedDatabaseIdentifier)=?" }.joined(separator: " AND ")
        return "DELETE FROM \(tableName.quotedDatabaseIdentifier) WHERE \(whereSQL)"
    }
}


// MARK: - ExistsQuery

private struct ExistsQuery {
    let tableName: String
    let conditionColumns: [String]
}

extension ExistsQuery {
    var sql: String {
        let whereSQL = conditionColumns.map { "\($0.quotedDatabaseIdentifier)=?" }.joined(separator: " AND ")
        return "SELECT 1 FROM \(tableName.quotedDatabaseIdentifier) WHERE \(whereSQL)"
    }
}

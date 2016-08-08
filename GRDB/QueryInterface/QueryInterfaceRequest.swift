/// A QueryInterfaceRequest describes an SQL query.
///
/// See https://github.com/groue/GRDB.swift#the-query-interface
public struct QueryInterfaceRequest<T> {
    let query: _SQLSelectQuery
    
    /// Initializes a QueryInterfaceRequest based on table *tableName*.
    ///
    /// It represents the SQL query `SELECT * FROM tableName`.
    public init(tableName: String) {
        self.init(query: _SQLSelectQuery(select: [_SQLResultColumn.star(nil)], from: .table(name: tableName, alias: nil)))
    }
    
    init(query: _SQLSelectQuery) {
        self.query = query
    }
}


extension QueryInterfaceRequest : FetchRequest {
    
    /// Returns a prepared statement that is ready to be executed.
    ///
    /// - throws: A DatabaseError whenever SQLite could not parse the sql query.
    public func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        // TODO: split statement generation from arguments building
        var arguments: StatementArguments? = StatementArguments()
        let sql = query.sql(&arguments)
        let statement = try db.makeSelectStatement(sql)
        try statement.setArgumentsWithValidation(arguments!)
        return (statement, nil)
    }
}


extension QueryInterfaceRequest where T: RowConvertible {
    
    // MARK: Fetching Record and RowConvertible
    
    /// Returns a sequence of values.
    ///
    ///     let nameColumn = Column("name")
    ///     let request = Person.order(nameColumn)
    ///     let persons = request.fetch(db) // DatabaseSequence<Person>
    ///
    /// The returned sequence can be consumed several times, but it may yield
    /// different results, should database changes have occurred between two
    /// generations:
    ///
    ///     let persons = request.fetch(db)
    ///     Array(persons).count // 3
    ///     db.execute("DELETE ...")
    ///     Array(persons).count // 2
    ///
    /// If the database is modified while the sequence is iterating, the
    /// remaining elements are undefined.
    public func fetch(_ db: Database) -> DatabaseSequence<T> {
        return T.fetch(db, self)
    }
    
    /// Returns an array of values fetched from a fetch request.
    ///
    ///     let nameColumn = Column("name")
    ///     let request = Person.order(nameColumn)
    ///     let persons = request.fetchAll(db) // [Person]
    ///
    /// - parameter db: A database connection.
    public func fetchAll(_ db: Database) -> [T] {
        return T.fetchAll(db, self)
    }
    
    /// Returns a single value fetched from a fetch request.
    ///
    ///     let nameColumn = Column("name")
    ///     let request = Person.order(nameColumn)
    ///     let person = request.fetchOne(db) // Person?
    ///
    /// - parameter db: A database connection.
    public func fetchOne(_ db: Database) -> T? {
        return T.fetchOne(db, self)
    }
}


extension QueryInterfaceRequest {
    
    // MARK: Request Derivation
    
    /// Returns a new QueryInterfaceRequest with a new net of selected columns.
    public func select(_ selection: _SQLSelectable...) -> QueryInterfaceRequest<T> {
        return select(selection)
    }
    
    /// Returns a new QueryInterfaceRequest with a new net of selected columns.
    public func select(_ selection: [_SQLSelectable]) -> QueryInterfaceRequest<T> {
        var query = self.query
        query.selection = selection
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a new QueryInterfaceRequest with a new net of selected columns.
    public func select(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<T> {
        return select(_SQLExpression.sqlLiteral(sql, arguments))
    }
    
    /// Returns a new QueryInterfaceRequest which returns distinct rows.
    public func distinct() -> QueryInterfaceRequest<T> {
        var query = self.query
        query.isDistinct = true
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *predicate* added to the
    /// eventual set of already applied predicates.
    public func filter(_ predicate: SQLExpressible) -> QueryInterfaceRequest<T> {
        var query = self.query
        if let whereExpression = query.whereExpression {
            query.whereExpression = .infixOperator("AND", whereExpression, predicate.sqlExpression)
        } else {
            query.whereExpression = predicate.sqlExpression
        }
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *predicate* added to the
    /// eventual set of already applied predicates.
    public func filter(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<T> {
        return filter(_SQLExpression.sqlLiteral(sql, arguments))
    }
    
    /// Returns a new QueryInterfaceRequest grouped according to *expressions*.
    public func group(_ expressions: SQLExpressible...) -> QueryInterfaceRequest<T> {
        return group(expressions)
    }
    
    /// Returns a new QueryInterfaceRequest grouped according to *expressions*.
    public func group(_ expressions: [SQLExpressible]) -> QueryInterfaceRequest<T> {
        var query = self.query
        query.groupByExpressions = expressions.map { $0.sqlExpression }
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a new QueryInterfaceRequest with a new grouping.
    public func group(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<T> {
        return group(_SQLExpression.sqlLiteral(sql, arguments))
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *predicate* added to the
    /// eventual set of already applied predicates.
    public func having(_ predicate: SQLExpressible) -> QueryInterfaceRequest<T> {
        var query = self.query
        if let havingExpression = query.havingExpression {
            query.havingExpression = (havingExpression && predicate).sqlExpression
        } else {
            query.havingExpression = predicate.sqlExpression
        }
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *sql* added to
    /// the eventual set of already applied predicates.
    public func having(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<T> {
        return having(_SQLExpression.sqlLiteral(sql, arguments))
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *orderings* added to
    /// the eventual set of already applied orderings.
    public func order(_ orderings: _SQLOrderable...) -> QueryInterfaceRequest<T> {
        return order(orderings)
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *orderings* added to
    /// the eventual set of already applied orderings.
    public func order(_ orderings: [_SQLOrderable]) -> QueryInterfaceRequest<T> {
        var query = self.query
        query.orderings = orderings
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a new QueryInterfaceRequest with the provided *sql* added to the
    /// eventual set of already applied orderings.
    public func order(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<T> {
        return order([_SQLExpression.sqlLiteral(sql, arguments)])
    }
    
    /// Returns a new QueryInterfaceRequest sorted in reversed order.
    public func reversed() -> QueryInterfaceRequest<T> {
        var query = self.query
        query.isReversed = !query.isReversed
        return QueryInterfaceRequest(query: query)
    }
    
    /// Returns a QueryInterfaceRequest which fetches *limit* rows, starting at
    /// *offset*.
    public func limit(_ limit: Int, offset: Int? = nil) -> QueryInterfaceRequest<T> {
        var query = self.query
        query.limit = _SQLLimit(limit: limit, offset: offset)
        return QueryInterfaceRequest(query: query)
    }
}


extension QueryInterfaceRequest {
    
    // MARK: Counting
    
    /// Returns the number of rows matched by the request.
    ///
    /// - parameter db: A database connection.
    public func fetchCount(_ db: Database) -> Int {
        return Int.fetchOne(db, QueryInterfaceRequest(query: query.countQuery))!
    }
}


extension QueryInterfaceRequest {
    
    // MARK: QueryInterfaceRequest as subquery
    
    /// Returns an SQL expression that checks the inclusion of a value in
    /// the results of another request.
    public func contains(_ element: SQLExpressible) -> _SQLExpression {
        return .inSubQuery(query, element.sqlExpression)
    }
    
    /// Returns an SQL expression that checks whether the receiver, as a
    /// subquery, returns any row.
    public func exists() -> _SQLExpression {
        return .exists(query)
    }
}


extension TableMapping {
    
    // MARK: Request Derivation
    
    /// Returns a QueryInterfaceRequest which fetches all rows in the table.
    public static func all() -> QueryInterfaceRequest<Self> {
        return QueryInterfaceRequest(tableName: databaseTableName)
    }
    
    /// Returns a QueryInterfaceRequest which selects *selection*.
    public static func select(_ selection: _SQLSelectable...) -> QueryInterfaceRequest<Self> {
        return all().select(selection)
    }
    
    /// Returns a QueryInterfaceRequest which selects *selection*.
    public static func select(_ selection: [_SQLSelectable]) -> QueryInterfaceRequest<Self> {
        return all().select(selection)
    }
    
    /// Returns a QueryInterfaceRequest which selects *sql*.
    public static func select(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<Self> {
        return all().select(sql: sql, arguments: arguments)
    }
    
    /// Returns a QueryInterfaceRequest with the provided *predicate*.
    public static func filter(_ predicate: SQLExpressible) -> QueryInterfaceRequest<Self> {
        return all().filter(predicate)
    }
    
    /// Returns a QueryInterfaceRequest with the provided *predicate*.
    public static func filter(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<Self> {
        return all().filter(sql: sql, arguments: arguments)
    }
    
    /// Returns a QueryInterfaceRequest sorted according to the
    /// provided *orderings*.
    public static func order(_ orderings: _SQLOrderable...) -> QueryInterfaceRequest<Self> {
        return all().order(orderings)
    }
    
    /// Returns a QueryInterfaceRequest sorted according to the
    /// provided *orderings*.
    public static func order(_ orderings: [_SQLOrderable]) -> QueryInterfaceRequest<Self> {
        return all().order(orderings)
    }
    
    /// Returns a QueryInterfaceRequest sorted according to *sql*.
    public static func order(sql: String, arguments: StatementArguments? = nil) -> QueryInterfaceRequest<Self> {
        return all().order(sql: sql, arguments: arguments)
    }
    
    /// Returns a QueryInterfaceRequest which fetches *limit* rows, starting at
    /// *offset*.
    public static func limit(_ limit: Int, offset: Int? = nil) -> QueryInterfaceRequest<Self> {
        return all().limit(limit, offset: offset)
    }
}


extension TableMapping {
    
    // MARK: Counting
    
    /// Returns the number of records.
    ///
    /// - parameter db: A database connection.
    public static func fetchCount(_ db: Database) -> Int {
        return all().fetchCount(db)
    }
}


extension RowConvertible where Self: TableMapping {
    
    // MARK: Fetching All
    
    /// Returns a sequence of all records fetched from the database.
    ///
    ///     let persons = Person.fetch(db) // DatabaseSequence<Person>
    ///
    /// The returned sequence can be consumed several times, but it may yield
    /// different results, should database changes have occurred between two
    /// generations:
    ///
    ///     let persons = Person.fetch(db)
    ///     Array(persons).count // 3
    ///     db.execute("DELETE ...")
    ///     Array(persons).count // 2
    ///
    /// If the database is modified while the sequence is iterating, the
    /// remaining elements are undefined.
    public static func fetch(_ db: Database) -> DatabaseSequence<Self> {
        return all().fetch(db)
    }
    
    /// Returns an array of all records fetched from the database.
    ///
    ///     let persons = Person.fetchAll(db) // [Person]
    ///
    /// - parameter db: A database connection.
    public static func fetchAll(_ db: Database) -> [Self] {
        return all().fetchAll(db)
    }
    
    /// Returns the first record fetched from a fetch request.
    ///
    ///     let person = Person.fetchOne(db) // Person?
    ///
    /// - parameter db: A database connection.
    public static func fetchOne(_ db: Database) -> Self? {
        return all().fetchOne(db)
    }
}

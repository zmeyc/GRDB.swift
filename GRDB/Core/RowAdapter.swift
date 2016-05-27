#if !SQLITE_HAS_CODEC
    #if os(OSX)
        import SQLiteMacOSX
    #elseif os(iOS)
        #if (arch(i386) || arch(x86_64))
            import SQLiteiPhoneSimulator
        #else
            import SQLiteiPhoneOS
        #endif
    #endif
#endif

// The types declared in this file are:
//
// - public struct RowAdapter
//
//     The public RowAdapter type
//
// - private protocol RowAdapterImpl
//
//      Protocol for inner implementations of RowAdapter:
//
//      - private struct IdentityRowAdapterImpl
//          Implementation for RowAdapter that performs no adapting
//
//      - private struct DictionaryRowAdapterImpl
//          Implementation for RowAdapter that maps column names with a dictionary
//
//      - private struct NestedRowAdapterImpl
//          Implementation for RowAdapter that holds a "main" adapter and a
//          dictionary of named adapters.
//
// - struct ColumnsAdapter
//
//     A RowAdapter itself can not do anything, because it doesn't know the
//     row layout. ColumnsAdapter is the product of a RowAdapter and the row
//     layout of a statement: it maps adapted columns to columns of the
//     "base row".
//
// - struct AdapterRowImpl
//
//     A RowImpl for adapter rows.
//
// - struct AdapterRowImpl.Binding
//
//     A struct that holds a "main" column adapter, and a dictionary
//     of named adapters.

/// Row adapters help two incompatible row interfaces to work together.
///
/// For example, a row consumer expects a column named "foo", but the produced
/// column has a column named "bar".
///
/// A row adapter performs that column mapping:
///
///     // An adapter that maps column 'bar' to column 'foo':
///     let adapter = RowAdapter(mapping: ["foo": "bar"])
///
///     // Fetch a column named 'bar', using adapter:
///     let row = Row.fetchOne(db, "SELECT 'Hello' AS bar", adapter: adapter)
///
///     // The adapter in action:
///     row.value(named: "foo") // "Hello"
///
/// A row adapter can also define "sub rows", that help several consumers feed
/// on a single row:
///
///     let sql = "SELECT books.*, persons.name AS authorName " +
///         "FROM books " +
///     "LEFT JOIN persons ON books.authorID = persons.id"
///
///     let adapter = RowAdapter(
///         mapping: ["id": "id", "title": "title"],
///         namedMappings: ["author": ["authorID": "id", "authorName": "name"]])
///
///     for row in Row.fetchAll(db, sql, adapter: adapter) {
///         // <Row id:1 title:"Moby-Dick">
///         print(row)
///
///         if let authorRow = row.adapted(for: "author") {
///             // <Row id:10 name:"Melville">
///             print(authorRow)
///         }
public struct RowAdapter {
    private let impl: RowAdapterImpl
    
    /// Creates an adapter that maps column names.
    ///
    ///     // An adapter that maps column 'produced' to column 'consumed':
    ///     let adapter = RowAdapter(mapping: ["consumed": "produced"])
    ///
    ///     // Fetch a column named 'produced', and apply adapter:
    ///     let row = Row.fetchOne(db, "SELECT 'Hello' AS produced", adapter: adapter)!
    ///
    ///     // The adapter in action:
    ///     row.value(named: "consumed") // "Hello"

    public init(mapping: [String: String]) {
        impl = DictionaryRowAdapterImpl(dictionary: mapping)
    }
    
    // Creates an adapter than does nothing.
    ///
    ///     let row = Row.fetchOne(db, "SELECT 'Hello' AS greeting", adapter: RowAdapter())!
    ///     row.value(named: "greeting") // "Hello"
    public init() {
        impl = IdentityRowAdapterImpl()
    }
    
    /// TODO
    public func adding(adapter: RowAdapter, named name: String) -> RowAdapter {
        return impl.adapterByAdding(adapter, named: name)
    }
    
    // TODO
    public mutating func add(adapter: RowAdapter, named name: String) {
        self = adding(adapter, named: name)
    }

    
//    /// Creates a row adapter with named adapter.
//    ///
//    /// For example:
//    ///
//    ///     let sql = "SELECT main.id AS mainID, p.name AS mainName, " +
//    ///               "       friend.id AS friendID, friend.name AS friendName, " +
//    ///               "FROM persons main " +
//    ///               "LEFT JOIN persons friend ON p.bestFriendID = f.id"
//    ///
//    ///     let mainMapping = ["id": "mainID", "name": "mainName"]
//    ///     let bestFriendMapping = ["id": "friendID", "name": "friendName"]
//    ///     let adapter = RowAdapter(
//    ///         main: RowAdapter(mapping: mainMapping),
//    ///         namedAdapters: ["bestFriend": RowAdapter(mapping: bestFriendMapping)])
//    ///
//    ///     for row in Row.fetchAll(db, sql, adapter: adapter) {
//    ///         print(row)                           // <Row id:1 name:"Arthur">
//    ///         print(row.adapted(for: "bestFriend")) // <Row id:2 name:"Barbara">
//    ///     }
//    ///
//    /// - parameters:
//    ///     - mapping: An eventual mapping to apply to rows; if nil, rows are
//    ///       left intact.
//    ///     - namedMappings: A dictionary of named mappings to be loaded with
//    ///       the row.adapted(for:) method.
//    public init(mainAdapter: RowAdapter? = nil, namedAdapters: [String: RowAdapter]) {
//        if let mainAdapter = mainAdapter {
//            GRDBPrecondition(!mainAdapter.hasNamedAdapters, "Invalid shadowing of named adapters defined by the main adapter")
//        }
//        
//        impl = NestedRowAdapterImpl(
//            mainRowAdapter: mainAdapter ?? RowAdapter(),
//            namedRowAdapters: namedAdapters)
//    }
    
    private init(impl: RowAdapterImpl) {
        self.impl = impl
    }
    
    func binding(with statement: SelectStatement) throws -> AdapterRowImpl.Binding {
        return try impl.binding(with: statement)
    }
    
    // Return an array [(baseRowIndex, mappedColumn), ...] ordered like the statement columns.
    private func columnBaseIndexes(statement statement: SelectStatement) throws -> [(Int, String)] {
        return try impl.columnBaseIndexes(statement: statement)
    }
    
    private var hasNamedAdapters: Bool {
        return impl.hasNamedAdapters
    }
}

extension RowAdapter: DictionaryLiteralConvertible {
    public init(dictionaryLiteral elements: (String, String)...) {
        let mapping = Dictionary(keyValueSequence: elements)
        self.init(mapping: mapping)
    }
}

private protocol RowAdapterImpl {
    // Return an array [(baseRowIndex, mappedColumn), ...] ordered like the statement columns.
    func columnBaseIndexes(statement statement: SelectStatement) throws -> [(Int, String)]
    
    // Named Bindings
    func namedBindings(statement statement: SelectStatement) throws -> [String: AdapterRowImpl.Binding]
    
    var hasNamedAdapters: Bool { get }
    
    func adapterByAdding(adapter: RowAdapter, named name: String) -> RowAdapter
}

extension RowAdapterImpl {
    // extension method
    func binding(with statement: SelectStatement) throws -> AdapterRowImpl.Binding {
        return try AdapterRowImpl.Binding(
            columnsAdapter: ColumnsAdapter(columnBaseIndexes: columnBaseIndexes(statement: statement)),
            namedBindings: namedBindings(statement: statement))
    }

    // default implementation
    func namedBindings(statement statement: SelectStatement) throws -> [String: AdapterRowImpl.Binding] {
        return [:]
    }
    
    // default implementation
    func adapterByAdding(adapter: RowAdapter, named name: String) -> RowAdapter {
        return RowAdapter(impl: NestedRowAdapterImpl(
            mainRowAdapter: RowAdapter(impl: self),
            namedRowAdapters: [name: adapter]))
    }
}

private struct IdentityRowAdapterImpl: RowAdapterImpl {
    func columnBaseIndexes(statement statement: SelectStatement) throws -> [(Int, String)] {
        return Array(statement.columnNames.enumerate())
    }
    
    var hasNamedAdapters: Bool {
        return false
    }
}

private struct DictionaryRowAdapterImpl: RowAdapterImpl {
    let dictionary: [String: String]

    func columnBaseIndexes(statement statement: SelectStatement) throws -> [(Int, String)] {
        return try dictionary
            .map { (mappedColumn, baseColumn) -> (Int, String) in
                guard let index = statement.indexOfColumn(named: baseColumn) else {
                    throw DatabaseError(code: SQLITE_MISUSE, message: "Mapping references missing column \(baseColumn). Valid column names are: \(statement.columnNames.joinWithSeparator(", ")).")
                }
                return (index, mappedColumn)
            }
            .sort { return $0.0 < $1.0 }
    }
    
    var hasNamedAdapters: Bool {
        return false
    }
}

private struct NestedRowAdapterImpl: RowAdapterImpl {
    let mainRowAdapter: RowAdapter
    let namedRowAdapters: [String: RowAdapter]
    
    func columnBaseIndexes(statement statement: SelectStatement) throws -> [(Int, String)] {
        return try mainRowAdapter.columnBaseIndexes(statement: statement)
    }
    
    func namedBindings(statement statement: SelectStatement) throws -> [String: AdapterRowImpl.Binding] {
        let namedBindings = try namedRowAdapters.map { (identifier: String, adapter: RowAdapter) -> (String, AdapterRowImpl.Binding) in
            let namedBinding = try AdapterRowImpl.Binding(
                columnsAdapter: ColumnsAdapter(columnBaseIndexes: adapter.columnBaseIndexes(statement: statement)),
                namedBindings: [:])
            return (identifier, namedBinding)
        }
        return Dictionary(keyValueSequence: namedBindings)
    }
    
    var hasNamedAdapters: Bool {
        return !namedRowAdapters.isEmpty
    }
    
    func adapterByAdding(adapter: RowAdapter, named name: String) -> RowAdapter {
        var namedRowAdapters = self.namedRowAdapters
        namedRowAdapters[name] = adapter
        return RowAdapter(impl: NestedRowAdapterImpl(mainRowAdapter: mainRowAdapter, namedRowAdapters: namedRowAdapters))
    }
}

struct ColumnsAdapter {
    let columnBaseIndexes: [(Int, String)]      // [(baseRowIndex, mappedColumn), ...]
    let lowercaseColumnIndexes: [String: Int]   // [mappedColumn: adaptedRowIndex]

    init(columnBaseIndexes: [(Int, String)]) {
        self.columnBaseIndexes = columnBaseIndexes
        self.lowercaseColumnIndexes = Dictionary(keyValueSequence: columnBaseIndexes.enumerate().map { ($1.1.lowercaseString, $0) })
    }

    var count: Int {
        return columnBaseIndexes.count
    }

    func baseColumIndex(adaptedIndex index: Int) -> Int {
        return columnBaseIndexes[index].0
    }

    func columnName(adaptedIndex index: Int) -> String {
        return columnBaseIndexes[index].1
    }

    func adaptedIndexOfColumn(named name: String) -> Int? {
        if let index = lowercaseColumnIndexes[name] {
            return index
        }
        return lowercaseColumnIndexes[name.lowercaseString]
    }
}

extension Row {
    /// Builds a row from a base row and an adapter binding
    convenience init(baseRow: Row, adapterBinding binding: AdapterRowImpl.Binding) {
        self.init(impl: AdapterRowImpl(baseRow: baseRow, binding: binding))
    }
    
    /// Returns self if adapter is nil
    func adaptedRow(adapter adapter: RowAdapter?, statement: SelectStatement) throws -> Row {
        guard let adapter = adapter else {
            return self
        }
        return try Row(baseRow: self, adapterBinding: adapter.binding(with: statement))
    }
}

// See Row.init(baseRow:binding:)
struct AdapterRowImpl : RowImpl {
    
    struct Binding {
        let columnsAdapter: ColumnsAdapter
        let namedBindings: [String: Binding]
    }
    
    let baseRow: Row
    let binding: Binding
    var columnsAdapter: ColumnsAdapter { return binding.columnsAdapter }
    
    init(baseRow: Row, binding: Binding) {
        self.baseRow = baseRow
        self.binding = binding
    }
    
    var count: Int {
        return columnsAdapter.count
    }
    
    func databaseValue(atIndex index: Int) -> DatabaseValue {
        return baseRow.databaseValue(atIndex: columnsAdapter.baseColumIndex(adaptedIndex: index))
    }
    
    func dataNoCopy(atIndex index:Int) -> NSData? {
        return baseRow.dataNoCopy(atIndex: columnsAdapter.baseColumIndex(adaptedIndex: index))
    }
    
    func columnName(atIndex index: Int) -> String {
        return columnsAdapter.columnName(adaptedIndex: index)
    }
    
    func indexOfColumn(named name: String) -> Int? {
        return columnsAdapter.adaptedIndexOfColumn(named: name)
    }
    
    func adapted(for name: String) -> Row? {
        guard let binding = binding.namedBindings[name] else {
            return nil
        }
        return Row(baseRow: baseRow, adapterBinding: binding)
    }
    
    var adaptationNames: Set<String> {
        return Set(binding.namedBindings.keys)
    }
    
    func copy(row: Row) -> Row {
        return Row(baseRow: baseRow.copy(), adapterBinding: binding)
    }
}

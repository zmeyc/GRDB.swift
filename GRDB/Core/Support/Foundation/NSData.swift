import Foundation

/// NSData is convertible to and from DatabaseValue.
extension NSData : DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    public var databaseValue: DatabaseValue {
        #if os(Linux)
        let data: Data = self.bridge()
        return data.databaseValue
        #else
        return (self as Data).databaseValue
        #endif
    }
    
    /// Returns an NSData initialized from *databaseValue*, if it contains
    /// a Blob.
    public static func fromDatabaseValue(_ databaseValue: DatabaseValue) -> Self? {
        if let data = Data.fromDatabaseValue(databaseValue) {
            #if os(Linux)
            // Error: constructing an object of class type 'Self' with a metatype value must use a 'required' initializer
            //return self.init(data: data)
            // Workaround:
            let coder = NSCoder()
            let data = NSData(data: data)
            data.encode(with: coder)
            return self.init(coder: coder)
            #else
            return self.init(data: data)
            #endif
        }
        return nil
    }
}

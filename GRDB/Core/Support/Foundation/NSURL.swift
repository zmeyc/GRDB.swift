import Foundation

/// NSURL adopts DatabaseValueConvertible.
extension NSURL : DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    /// (the URL's absoluteString).
    public var databaseValue: DatabaseValue {
        #if os(Linux)
        let url: URL = self.bridge()
        return url.databaseValue
        #else
        return (self as URL).databaseValue
        #endif
    }
    
    /// Returns an NSURL initialized from *databaseValue*, if possible.
    public static func fromDatabaseValue(_ databaseValue: DatabaseValue) -> Self? {
        guard let url = URL.fromDatabaseValue(databaseValue) else {
            return nil
        }
        #if os(Linux)
        // Error: constructing an object of class type 'Self' with a metatype value must use a 'required' initializer
        //return self.init(string: url.absoluteString ?? "")
        // Workaround:
        let coder = NSCoder()
        guard let nsurl = NSURL(string: url.absoluteString ?? "") else {
            return nil
        }
        nsurl.encode(with: coder)
        return self.init(coder: coder)
        #else
        return self.init(string: url.absoluteString)
        #endif
    }
}

import UIKit

protocol PersonEditionViewControllerDelegate: class {
    func personEditionControllerDidComplete(_ controller: PersonEditionViewController)
}

class PersonEditionViewController: UITableViewController {
    weak var delegate: PersonEditionViewControllerDelegate?
    var person: Person! { didSet { configureView() } }
    var cancelButtonHidden: Bool = false { didSet { configureView() } }
    var commitButtonHidden: Bool = false { didSet { configureView() } }

    @IBOutlet private weak var cancelBarButtonItem: UIBarButtonItem!
    @IBOutlet private weak var commitBarButtonItem: UIBarButtonItem!
    @IBOutlet private weak var nameCell: UITableViewCell!
    @IBOutlet private weak var nameTextField: UITextField!
    @IBOutlet private weak var scoreCell: UITableViewCell!
    @IBOutlet private weak var scoreTextField: UITextField!
    
    func applyChanges() {
        if let name = nameTextField.text, !name.isEmpty {
            person.name = name
        }
        person.score = scoreTextField.text.flatMap { Int($0) } ?? 0
    }
    
    override func viewDidLoad() {
        super.viewDidLoad()
        configureView()
    }
    
    private func configureView() {
        guard isViewLoaded else { return }
        
        nameTextField.text = person.name
        if person.score == 0 && person.id == nil {
            scoreTextField.text = ""
        } else {
            scoreTextField.text = "\(person.score)"
        }
    
        if cancelButtonHidden {
            navigationItem.leftBarButtonItem = nil
        } else {
            navigationItem.leftBarButtonItem = cancelBarButtonItem
        }

        if cancelButtonHidden {
            navigationItem.rightBarButtonItem = nil
        } else {
            navigationItem.rightBarButtonItem = commitBarButtonItem
        }
    }
}


// MARK: - Navigation

extension PersonEditionViewController {
    
    override func shouldPerformSegue(withIdentifier identifier: String, sender: AnyObject?) -> Bool {
        // Force keyboard to dismiss early
        view.endEditing(true)
        return true
    }
    
    override func willMove(toParentViewController parent: UIViewController?) {
        super.willMove(toParentViewController: parent)
        
        if parent == nil {
            // Self is popping from its navigation controller
            delegate?.personEditionControllerDidComplete(self)
        }
    }
    
}


// MARK: - Form

extension PersonEditionViewController: UITextFieldDelegate {
    
    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        nameTextField.becomeFirstResponder()
    }
    
    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: false)
        let cell = tableView.cellForRow(at: indexPath)
        if cell === nameCell {
            nameTextField.becomeFirstResponder()
        } else if cell === scoreCell {
            scoreTextField.becomeFirstResponder()
        }
    }
    
    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        if textField == nameTextField {
            scoreTextField.becomeFirstResponder()
        }
        return false
    }
}

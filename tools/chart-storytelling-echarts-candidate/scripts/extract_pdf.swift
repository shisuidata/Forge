import Foundation
import PDFKit

let arguments = CommandLine.arguments
if arguments.count != 2 {
    fatalError("usage: extract_pdf.swift input.pdf")
}
guard let document = PDFDocument(url: URL(fileURLWithPath: arguments[1])) else {
    fatalError("cannot open PDF")
}
print("PAGES=\(document.pageCount)")
print(document.string ?? "")

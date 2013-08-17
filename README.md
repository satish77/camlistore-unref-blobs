camlistore-unref-blobs
======================

Tool to search for unreferenced/orphaned blobs in camlistore.

Unreferenced blobs in camlistore(camlilstore.org) are blobs which cannot be reached from a permanode or a claim on a permanode.
The tool visits each blobs starting at each permanode and increments reference count of each blob.
Finally, blobs with zero referenced count are printed out.

TODO:
1. Test the logic using different usage scenarios.
2. Request for comments from authors and user community of camlistore.
3. Make it more robust and usable.



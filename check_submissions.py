import os.path
import subprocess
from subprocess import STDOUT, PIPE
import csv
import zipfile
import math
import argparse
import psycopg2
import sys
import shutil

# put this script in the project's root (same as the pom.xml file)
# change as fit to the computer
projectPath = "."
solutionPath = "."
zips_folder = "zips"
checked_pdfs_folder = "checked_pdfs"
username, password, db = "roei217", "6262172", "cs263363"

# DO NOT CHANGE
feedbacks_folder = "feedbacks"
failed_folder = "failed"
tmp_folder = "tmp"
success_folder = "success"
pdfs_folder = "pdfs"
solutions_folder = "solutions"
final_feedbacks_folder = "final_feedbacks"


# run a command in cmd
def run(command, file):
    return subprocess.call(command, stdout=file, stderr=file)


# reset last submission checked in order to prepare for the next one
def removeAndResetLastSubmission(id1, id2=0):
    ret = 0
    if os.path.exists(os.path.join(solutionPath, 'Solution.py')):
        path_to_move = os.path.join(projectPath, solutions_folder)
        if id2 == 0:
            os.rename(os.path.join(solutionPath, 'Solution.py'), os.path.join(path_to_move, str(id1) + '.py'))
        else:
            os.rename(os.path.join(solutionPath, 'Solution.py'),
                      os.path.join(path_to_move, str(id1) + '_' + str(id2) + '.py'))
    # reset db in case student forgot
    try:
        conn = psycopg2.connect(host="localhost", dbname=db, user=username, password=password)
        conn.set_isolation_level(0)
    except:
        print("Unable to connect to the database.")
        ret = -1
    cur = conn.cursor()
    try:
        cur.execute("SELECT tablename from pg_tables where schemaname = 'public';")
        rows = cur.fetchall()
        for row in rows:
            # print("dropping table:", row[0])
            cur.execute("drop table if exists " + row[0] + " cascade")
            ret = 1

        cur.execute("SELECT viewname from pg_views where schemaname = 'public';")
        rows = cur.fetchall()
        for row in rows:
            # print("dropping view:", row[0])
            cur.execute("drop view if exists " + row[0] + " cascade")
            ret = 1

        cur.close()
        conn.close()
    except:
        print("Error: ", sys.exc_info()[1])
        ret = -1
    return ret


# unzips a zip according to the following rules:
# zip format must be ID.zip contains ID.txt, ID.pdf, Solution.py
def unzipSingle(zip_file):
    ids = zip_file.split('.zip')[0]
    id1 = ids.split("\\")[1]
    if len(id1) != 9:
        return 0
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        list_Of_files = zip_ref.namelist()
        if len(list_Of_files) != 3:
            return 0
        if "Solution.py" not in list_Of_files or id1 + ".pdf" not in list_Of_files or id1 + ".txt" not in list_Of_files:
            return 0
        zip_ref.extractall(tmp_folder)
        zip_ref.close()
        os.replace(os.path.join(tmp_folder, "Solution.py"), os.path.join(solutionPath, "Solution.py"))
        if os.path.isdir(pdfs_folder) is False:
            os.mkdir(pdfs_folder)
        os.replace(os.path.join(tmp_folder, id1 + ".pdf"), os.path.join(pdfs_folder, id1 + ".pdf"))
        if os.path.exists(os.path.join(tmp_folder, id1 + ".txt")):
            os.remove(os.path.join(tmp_folder, id1 + ".txt"))
    return id1


# unzips a zip according to the following rules:
# zip format must be ID1-ID2.zip contains ID1_ID2.txt, ID1_ID2.pdf, Solution.py
def unzip(zip_file):
    ids = zip_file.split('.zip')[0]
    try:
        id1, id2 = ids.split("\\")[1].split("-")[0], ids.split("\\")[1].split("-")[1]
    except:
        id1, id2 = ids.split("\\")[1].split("_")[0], ids.split("\\")[1].split("_")[1]
    if len(id1) != 9 or len(id2) != 9:
        return 0, 0
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        list_Of_files = zip_ref.namelist()
        if len(list_Of_files) != 3:
            return 0, 0
        if "Solution.py" not in list_Of_files or id1 + "_" + id2 + ".pdf" not in list_Of_files or id1 + "_" + id2 + \
                ".txt" not in list_Of_files:
            return 0, 0
        zip_ref.extractall(tmp_folder)
        zip_ref.close()
        os.replace(os.path.join(tmp_folder, "Solution.py"), os.path.join(solutionPath, "Solution.py"))
        if os.path.isdir(pdfs_folder) is False:
            os.mkdir(pdfs_folder)
        os.replace(os.path.join(tmp_folder, id1 + "_" + id2 + ".pdf"),
                   os.path.join(pdfs_folder, id1 + "_" + id2 + ".pdf"))
        if os.path.exists(os.path.join(tmp_folder, id1 + "_" + id2 + ".txt")):
            os.remove(os.path.join(tmp_folder, id1 + "_" + id2 + ".txt"))
    return id1, id2


# zips all
def zipAll():
    if os.path.isdir(final_feedbacks_folder) is False:
        os.mkdir(final_feedbacks_folder)
    txts = [f for f in os.listdir(feedbacks_folder) if os.path.isfile(os.path.join(feedbacks_folder, f))]
    for txt in txts:
        ids = txt.split('.')[0]
        if os.path.isfile(os.path.join(checked_pdfs_folder, ids + ".pdf")):
            with zipfile.ZipFile(os.path.join(final_feedbacks_folder, ids + ".zip"), 'w') as zipped:
                zipped.write(os.path.join(feedbacks_folder, ids + ".txt"), arcname=ids + ".txt",
                             compress_type=zipfile.ZIP_DEFLATED)
                zipped.write(os.path.join(checked_pdfs_folder, ids + ".pdf"), arcname=ids + ".pdf",
                             compress_type=zipfile.ZIP_DEFLATED)
            os.remove(os.path.join(feedbacks_folder, ids + ".txt"))
            os.remove(os.path.join(checked_pdfs_folder, ids + ".pdf"))
        else:
            print('You are missing checked pdf for ' + ids)


# cleanup of files for the next iteration
def testingFailed(zip_name):
    os.replace(zip_name, os.path.join(failed_folder, zip_name.split('\\')[1]))
    files = [f for f in os.listdir(tmp_folder) if os.path.isfile(os.path.join(tmp_folder, f))]
    for f in files:
        os.remove(os.path.join(tmp_folder, f))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-doubles", "--doubles", help="submission in doubles, default singles", action="store_true")
    args = parser.parse_args()
    doubles = args.doubles
    # prepare needed folders
    if os.path.isdir(failed_folder) is False:
        os.mkdir(failed_folder)
    if os.path.isdir(tmp_folder) is False:
        os.mkdir(tmp_folder)
    if os.path.isdir(zips_folder) is False:
        os.mkdir(zips_folder)
    if os.path.isdir(checked_pdfs_folder) is False:
        os.mkdir(checked_pdfs_folder)
    if os.path.isdir(solutions_folder) is False:
        os.mkdir(solutions_folder)
    # retrieve all zips to check
    zips = [f for f in os.listdir(zips_folder) if os.path.isfile(os.path.join(zips_folder, f))]
    zips = [os.path.join(zips_folder, f) for f in zips]
    # run output

    # prepare an excel csv
    with open(os.path.join('grades.csv'), 'w', newline='') as csvfile:
        csv_object = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

        stats = dict()
        avg_grade, valid_zips = 0, 0

        # check each zip file
        for zip_path in zips:
            if doubles is True:
                # unzip the zip for 2 submitters
                id_1, id_2 = unzip(zip_path)
                if id_1 == 0 or id_2 == 0:
                    testingFailed(zip_path)
                    print("Problem unzipping")
                    continue
                print("Testing " + str(id_1) + ", " + str(id_2))
            else:
                # unzip the zip for 1 submitter
                id_1 = unzipSingle(zip_path)
                if id_1 == 0:
                    testingFailed(zip_path)
                    print("Problem unzipping")
                    continue
                print("Testing " + str(id_1))

            # prepare feedbacks folder
            if os.path.isdir(feedbacks_folder + "_tmp") is False:
                os.mkdir(feedbacks_folder + "_tmp")

            # prepare feedbacks folder
            if os.path.isdir(feedbacks_folder) is False:
                os.mkdir(feedbacks_folder)

            # run tests
            if doubles:
                f = open(os.path.join(feedbacks_folder + "_tmp", str(id_1) + "_" + str(id_2) + ".txt"), "w+")
                res = run("python -m unittest -v Tests.gradingTest.Test", f)
                f.close()
                f = open(os.path.join(feedbacks_folder + "_tmp", str(id_1) + "_" + str(id_2) + ".txt"), "r")
            else:
                f = open(os.path.join(feedbacks_folder + "_tmp", str(id_1) + ".txt"), "w+")
                res = run("python -m unittest -v Tests.gradingTest.Test", f)
                f.close()
                f = open(os.path.join(feedbacks_folder + "_tmp", str(id_1) + ".txt"), "r")

            tests = []
            passed_tests, failed_tests = [], []
            for test in f:
                if test == '\n':
                    break
                test_name = test.split(' ')[0]
                result = test.split(' ... ')[1]
                tests.append((test_name, True if "ok" in result else False))
            sorted(tests)
            f.close()

            # first iteration
            if (not stats) is True:
                stats["drop tables"] = 0
                csv_object.writerow(['ID'] + [x[0] for x in tests] + ['Wet', 'Dry', 'Final'])
                for test in tests:
                    stats[test[0]] = 0

            for test in tests:
                if test[1]:
                    passed_tests.append(test[0])
                else:
                    failed_tests.append(test[0])
            assert (len(tests) == len(passed_tests) + len(failed_tests))

            personal_stats = dict()

            # reset previous submission to check the next
            if doubles is True:
                res = removeAndResetLastSubmission(id_1, id_2)
            else:
                res = removeAndResetLastSubmission(id_1)
            if res == -1:
                testingFailed(zip_path)
                print("Problem resetting")
                continue

            # output a text file with each student's result to feedbacks
            if doubles is True:
                f = open(os.path.join(feedbacks_folder, str(id_1) + "_" + str(id_2) + ".txt"), "w")
            else:
                f = open(os.path.join(feedbacks_folder, str(id_1) + ".txt"), "w")
            for test in tests:
                if test[0] in passed_tests:
                    f.write(
                        test[0] + " " * (45 - len(test[0])) + "Passed\n")
                    personal_stats[test[0]] = 1
                    try:
                        stats[test[0]] += 1
                    except:
                        print("Problem generating feedback, run manual")
                else:
                    f.write(
                        test[0] + " " * (45 - len(test[0])) + "Failed\n")
                    personal_stats[test[0]] = 0

            if res == 1:
                f.write("drop tables " + " " * (45 - len("drop tables ")) + "Failed\n")
                personal_stats["drop tables"] = 0
            else:
                f.write("drop tables " + " " * (45 - len("drop tables ")) + "Passed\n")
                personal_stats["drop tables"] = 1
                try:
                    stats["drop tables"] += 1
                except:
                    print("Problem generating feedback, run manual")

            grade = str(math.ceil(len(passed_tests) * 100 / len(tests)) - 5 * res)
            f.write("\n\n\n" + " " * 10 + "Final Grade" + "   " + grade)
            f.close()

            # transfer the zip to success folder
            if os.path.isdir(success_folder) is False:
                os.mkdir(success_folder)

            os.replace(zip_path, os.path.join(success_folder, zip_path.split('\\')[1]))
            personal_res = [value for key, value in sorted(personal_stats.items(), key=lambda x: x[0])]
            csv_object.writerow([id_1] + personal_res + [grade, '', ''])
            if doubles is True:
                csv_object.writerow([id_2] + personal_res + [grade, '', ''])
            avg_grade += int(grade)
            valid_zips += 1
            print('Final is ' + grade)

        final_stats = [str(int(float(value) / valid_zips * 100)) for key, value in
                       sorted(stats.items(), key=lambda x: x[0])]
        csv_object.writerow(['Final'] + final_stats + [float(avg_grade) / valid_zips, '', ''])

    shutil.rmtree(tmp_folder, ignore_errors=True)

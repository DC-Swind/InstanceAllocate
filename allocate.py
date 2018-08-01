import numpy as np
from collections import Counter
import time

def load_data(limit_file, app_file, instance_file, machine_file):
    analysis = {}
    analysis["app_disk"] = []
    analysis["app_mem"] = 99999
    analysis["app_cpu"] = 99999
    analysis["app_cpu_max"] = 0
    analysis["machine_disk"] = []
    analysis["machine_cpu"] = []
    analysis["machine_P"] = []
    analysis["machine_M"] = []
    analysis["machine_PM"] = []

    f = open(limit_file, "r")
    lines = f.readlines()
    f.close()
    app_id1 = []
    app_id2 = []
    k = []
    for line in lines:
        a, b, c = line.split(",")
        app_id1.append(int(a.split("_")[1]))
        app_id2.append(int(b.split("_")[1]))
        k.append(int(c))
    print "read", len(app_id1), "limit lines"

    f = open(app_file, "r")
    lines = f.readlines()
    f.close()
    app_resources = {}
    for line in lines:
        sp = line.split(",")
        app_id = int(sp[0].split("_")[1])
        cpus = [float(num) for num in sp[1].split("|")]
        analysis["app_cpu"] = min(analysis["app_cpu"], max(cpus))
        analysis["app_cpu_max"] = max(analysis["app_cpu_max"], max(cpus))
        mems = [float(num) for num in sp[2].split("|")]
        analysis["app_mem"] = min(analysis["app_mem"], max(mems))
        disk = int(float(sp[3]))
        analysis["app_disk"].append(disk)
        P = int(sp[4])
        M = int(sp[5])
        PM = int(sp[6])
        app_resources[app_id] = {"cpus":cpus, "mems":mems, "disk":disk, "P":P, "M":M, "PM":PM}
    print "read", len(app_resources), "app lines"

    f = open(instance_file, "r")
    lines = f.readlines()
    f.close()
    instances = {}
    c1 =  0
    for line in lines:
        sp = line.split(",")
        inst_id = int(sp[0].split("_")[1])
        app_id = int(sp[1].split("_")[1])
        if len(sp)!=3:
            print sp

        try:
            machine_id = int(sp[2].split("_")[1])
            c1 += 1
        except:
            machine_id = -1
        instances[inst_id] = {"app_id":app_id, "machine_id":machine_id}
    print "read", len(instances), "instance lines", c1

    f = open(machine_file, "r")
    lines = f.readlines()
    f.close()
    machines = {}
    for line in lines:
        sp = line.split(",")
        machine_id = int(sp[0].split("_")[1])
        cpu = float(sp[1])
        analysis["machine_cpu"].append(cpu)
        mem = float(sp[2])
        disk = int(sp[3])
        analysis["machine_disk"].append(disk)
        P = int(sp[4])
        analysis["machine_P"].append(P)
        M = int(sp[5])
        analysis["machine_M"].append(M)
        PM = int(sp[6])
        analysis["machine_PM"].append(PM)

        machines[machine_id] = {"cpu":cpu, "mem":mem, "disk":disk, "P":P, "M":M, "PM":PM}
    print "read", len(machines), "machine lines"

    print "app disk",Counter(analysis["app_disk"])
    #print Counter(analysis["machine_disk"])
    print "machine cpu", Counter(analysis["machine_cpu"])
    print Counter(app_id2)
    #print Counter(analysis["machine_P"])
    #print Counter(analysis["machine_M"])
    #print Counter(analysis["machine_PM"])
    print "min app mem", analysis["app_mem"]
    print "min app cpu", analysis["app_cpu"]
    print "max app cpu", analysis["app_cpu_max"]

    return app_id1, app_id2, k, app_resources, instances, machines

def can_allocate(inst, inst_info, machine, limits, machine_id, cpu_idle_rate=0.5):
    if max(inst["mems"] - machine["mem"]) > 0:
        return False
    if inst["disk"] > machine["disk"]:
        return False
    if inst["P"] > machine["P"]:
        return False
    if inst["M"] > machine["M"]:
        return False
    if inst["PM"] > machine["PM"]:
        return False
    if min(machine["cpu"] - inst["cpus"]) / machine["total_cpu"] < cpu_idle_rate:
        return False

    allocate_insts = machine["insts"]
    machine_limit = {}
    for inst_id in allocate_insts:
        app_id = inst_info[inst_id]["app_id"]
        if limits.get(app_id) is None:
            continue
        limit = limits[app_id]
        for lm in limit:
            app_id2, k = lm
            if machine_limit.get(app_id2) is None:
                machine_limit[app_id2] = k if app_id != app_id2 else k+1
            else:
                machine_limit[app_id2] = min(machine_limit[app_id2], k if app_id != app_id2 else k+1)

    for inst_id in allocate_insts:
        app_id = inst_info[inst_id]["app_id"]
        if machine_limit.get(app_id) is not None:
            machine_limit[app_id] -= 1
            if machine_limit[app_id] < 0:
                print machine_limit[app_id], app_id
                print machine_id
                print allocate_insts
                print [inst_info[ins]["app_id"] for ins in allocate_insts]
            assert(machine_limit[app_id] >= 0)

    if machine_limit.get(inst["app_id"]) is not None and machine_limit[inst["app_id"]] <= 0:
        return False

    # check whether current app_id conflict other app
    app_id = inst["app_id"]
    if limits.get(app_id) is None:
        return True
    limit = limits[app_id]
    lim = {}
    for lm in limit:
        app_id2, k = lm
        if lim.get(app_id2) is None:
            lim[app_id2] = k
        else:
            lim[app_id2] = min(lim[app_id2], k)
    for inst_id in allocate_insts:
        app_id = inst_info[inst_id]["app_id"]
        if lim.get(app_id) is not None:
            lim[app_id] -= 1
            if lim[app_id] < 0:
                return False

    return True


def construct_limit_dict(app_id1, app_id2, k):
    rt = {}
    for i in range(len(app_id1)):
        if rt.get(app_id1[i]) is None:
            rt[app_id1[i]] = [(app_id2[i], k[i])]
        else:
            rt[app_id1[i]].append((app_id2[i], k[i]))
    return rt

def check_conflict(machine_id, machine, inst_info, limits):
    allocate_insts = machine["insts"]
    machine_limit = {}
    for inst_id in allocate_insts:
        app_id = inst_info[inst_id]["app_id"]
        if limits.get(app_id) is None:
            continue
        limit = limits[app_id]
        for lm in limit:
            app_id2, k = lm
            if machine_limit.get(app_id2) is None:
                machine_limit[app_id2] = k if app_id != app_id2 else k+1
            else:
                machine_limit[app_id2] = min(machine_limit[app_id2], k if app_id != app_id2 else k+1)

    for inst_id in allocate_insts:
        app_id = inst_info[inst_id]["app_id"]
        if machine_limit.get(app_id) is not None:
            machine_limit[app_id] -= 1
            if machine_limit[app_id] < 0:
                #print machine_limit[app_id], app_id
                #print machine_id
                #print allocate_insts
                #print [inst_info[ins]["app_id"] for ins in allocate_insts]
                return inst_id
            #assert(machine_limit[app_id] >= 0) # 1443, 496, 6398, 6455, 5942, 9100, 5889,
    return -1


def check_overload(machine_id, machine, inst_info, limits):
    allocate_insts = machine["insts"]

    if min(machine["cpu"]) / machine["total_cpu"] >= 0.5:
        return -1

    max_v = 0
    max_j = -1
    for inst_id in allocate_insts:
        if max(inst_info[inst_id]["cpus"]) > max_v:
            max_v = max(inst_info[inst_id]["cpus"])
            max_j = inst_id
    if max_j != -1:
        return max_j
    return machine["insts"][0]
"""

def check_overload(machine_id, machine, inst_info, limits):
    allocate_insts = machine["insts"]

    if min(machine["cpu"]) / machine["total_cpu"] >= 0.5:
        return -1

    min_v = 999999
    min_j = -1
    for inst_id in allocate_insts:
        if max(inst_info[inst_id]["cpus"]) < min_v:
            min_v = max(inst_info[inst_id]["cpus"])
            min_j = inst_id
    if min_j != -1:
        return min_j
    return machine["insts"][0]
"""

def deal_with_conflict(machine_info, inst_info, allocate_list, limits, new_machines):
    c1 = 0
    c2 = 0
    for machine_id, value in machine_info.items():
        while True:
            conflict_inst_id = check_conflict(machine_id, value, inst_info, limits)
            if conflict_inst_id == -1:
                break
            inst_info[conflict_inst_id]["allocated"] = False
            value["cpu"] += inst_info[conflict_inst_id]["cpus"]
            value["mem"] += inst_info[conflict_inst_id]["mems"]
            value["disk"] += inst_info[conflict_inst_id]["disk"]
            value["P"] += inst_info[conflict_inst_id]["P"]
            value["M"] += inst_info[conflict_inst_id]["M"]
            value["PM"] += inst_info[conflict_inst_id]["PM"]
            l = []
            for ins in value["insts"]:
                if ins != conflict_inst_id:
                    l.append(ins)
            value["insts"] = l

            for new_machine_id in new_machines:
                if can_allocate(inst_info[conflict_inst_id], inst_info, machine_info[new_machine_id], limits, new_machine_id, cpu_idle_rate=0.5):
                    inst_info[conflict_inst_id]["allocated"] = True
                    machine_info[new_machine_id]["cpu"] -= inst_info[conflict_inst_id]["cpus"]
                    machine_info[new_machine_id]["mem"] -= inst_info[conflict_inst_id]["mems"]
                    machine_info[new_machine_id]["disk"] -= inst_info[conflict_inst_id]["disk"]
                    machine_info[new_machine_id]["P"] -= inst_info[conflict_inst_id]["P"]
                    machine_info[new_machine_id]["M"] -= inst_info[conflict_inst_id]["M"]
                    machine_info[new_machine_id]["PM"] -= inst_info[conflict_inst_id]["PM"]
                    machine_info[new_machine_id]["insts"].append(conflict_inst_id)
                    allocate_list.append((conflict_inst_id, new_machine_id))
                    break
            if inst_info[conflict_inst_id]["allocated"]:
                c2 += 1
            c1 += 1

    print "solve", c2, "conflict, total:", c1

def allocate_epoch(machine_list, inst_list, inst_info, machine_info, limits, allocate_list,
                   c1, total_inst, cpu_idle_rate=0.5):
    index = 0
    cpt = time.time()
    for machine_id in machine_list:
        for inst_id in inst_list:
            if not inst_info[inst_id]["allocated"]:
                if can_allocate(inst_info[inst_id], inst_info, machine_info[machine_id], limits, machine_id, cpu_idle_rate):
                    allocate_list.append((inst_id, machine_id))
                    inst_info[inst_id]["allocated"] = True
                    machine_info[machine_id]["cpu"] -= inst_info[inst_id]["cpus"]
                    machine_info[machine_id]["mem"] -= inst_info[inst_id]["mems"]
                    machine_info[machine_id]["disk"] -= inst_info[inst_id]["disk"]
                    machine_info[machine_id]["P"] -= inst_info[inst_id]["P"]
                    machine_info[machine_id]["M"] -= inst_info[inst_id]["M"]
                    machine_info[machine_id]["PM"] -= inst_info[inst_id]["PM"]
                    machine_info[machine_id]["insts"].append(inst_id)

                    c1 += 1
                    if c1 == total_inst:
                        break
                    if machine_info[machine_id]["disk"] < 40:
                        break
                    if min(machine_info[machine_id]["mem"]) < 1.0:
                        break
                    #if float(machine_info[machine_id]["cpu"]) / machine_info[machine_id]["total_cpu"] < 0.505:
                    if min(machine_info[machine_id]["cpu"]) / machine_info[machine_id]["total_cpu"] < 0.005 + cpu_idle_rate:
                        break
        index += 1
        if index % 100 == 0:
            print index, time.time() - cpt, "allocated:", c1 , "/", total_inst
            cpt = time.time()
        if c1 == total_inst:
            print "all instance allocated."
            break
    return c1

def deal_with_overload(machine_info, inst_info, allocate_list, limits, new_machines):
    c1 = 0
    c2 = 0
    for machine_id, value in machine_info.items():
        while True:
            conflict_inst_id = check_overload(machine_id, value, inst_info, limits)
            if conflict_inst_id == -1:
                break
            inst_info[conflict_inst_id]["allocated"] = False
            value["cpu"] += inst_info[conflict_inst_id]["cpus"]
            value["mem"] += inst_info[conflict_inst_id]["mems"]
            value["disk"] += inst_info[conflict_inst_id]["disk"]
            value["P"] += inst_info[conflict_inst_id]["P"]
            value["M"] += inst_info[conflict_inst_id]["M"]
            value["PM"] += inst_info[conflict_inst_id]["PM"]
            l = []
            for ins in value["insts"]:
                if ins != conflict_inst_id:
                    l.append(ins)
            value["insts"] = l

            for new_machine_id in new_machines:
                if can_allocate(inst_info[conflict_inst_id], inst_info, machine_info[new_machine_id], limits, new_machine_id, cpu_idle_rate=0.5):
                    inst_info[conflict_inst_id]["allocated"] = True
                    machine_info[new_machine_id]["cpu"] -= inst_info[conflict_inst_id]["cpus"]
                    machine_info[new_machine_id]["mem"] -= inst_info[conflict_inst_id]["mems"]
                    machine_info[new_machine_id]["disk"] -= inst_info[conflict_inst_id]["disk"]
                    machine_info[new_machine_id]["P"] -= inst_info[conflict_inst_id]["P"]
                    machine_info[new_machine_id]["M"] -= inst_info[conflict_inst_id]["M"]
                    machine_info[new_machine_id]["PM"] -= inst_info[conflict_inst_id]["PM"]
                    machine_info[new_machine_id]["insts"].append(conflict_inst_id)
                    allocate_list.append((conflict_inst_id, new_machine_id))
                    break
            if inst_info[conflict_inst_id]["allocated"]:
                c2 += 1
            c1 += 1

    print "solve", c2, "overload, total:", c1

def allocate(app_id1, app_id2, k, app_resources, instances, machines):
    inst_info = {}
    machine_info = {}
    limits = construct_limit_dict(app_id1, app_id2, k)
    for inst_id, value in instances.items():
        app_id = value["app_id"]
        machine_id = value["machine_id"]
        # {"cpus":cpus, "mems":mems, "disk":disk, "P":P, "M":M, "PM":PM}
        inst_info[inst_id] = \
            {"cpus":np.array(app_resources[app_id]["cpus"]),
             "mems":np.array(app_resources[app_id]["mems"]),
             "disk":app_resources[app_id]["disk"],
             "P":app_resources[app_id]["P"],
             "M":app_resources[app_id]["M"],
             "PM":app_resources[app_id]["PM"],
             "app_id": app_id,
             "allocated": False if machine_id == -1 else True
            }
        if machine_id != -1:
            if machine_info.get(machine_id) is None:
                machine_info[machine_id] = \
                    {"cpu": np.array([float(machines[machine_id]["cpu"])]*98) - np.array(app_resources[app_id]["cpus"]),
                     "total_cpu": machines[machine_id]["cpu"],
                     "mem": np.array([float(machines[machine_id]["mem"])]*98) - np.array(app_resources[app_id]["mems"]),
                     "disk":machines[machine_id]["disk"] - app_resources[app_id]["disk"],
                     "P": machines[machine_id]["P"] - app_resources[app_id]["P"],
                     "M": machines[machine_id]["M"] - app_resources[app_id]["M"],
                     "PM": machines[machine_id]["PM"] - app_resources[app_id]["PM"],
                     "insts": [inst_id]
                    }
            else:
                machine_info[machine_id]["cpu"] -= np.array(app_resources[app_id]["cpus"])
                machine_info[machine_id]["mem"] -= np.array(app_resources[app_id]["mems"])
                machine_info[machine_id]["disk"] -= app_resources[app_id]["disk"]
                machine_info[machine_id]["P"] -= app_resources[app_id]["P"]
                machine_info[machine_id]["M"] -= app_resources[app_id]["M"]
                machine_info[machine_id]["PM"] -= app_resources[app_id]["PM"]
                machine_info[machine_id]["insts"].append(inst_id)
    #print len(machine_info)
    for machine_id, value in machines.items():
        if machine_info.get(machine_id) is None:
            machine_info[machine_id] = \
                {"cpu": np.array([float(machines[machine_id]["cpu"])]*98),
                 "total_cpu": machines[machine_id]["cpu"],
                 "mem": np.array([float(machines[machine_id]["mem"])]*98),
                 "disk":machines[machine_id]["disk"],
                 "P": machines[machine_id]["P"],
                 "M": machines[machine_id]["M"],
                 "PM": machines[machine_id]["PM"],
                 "insts": []
                }
    #print len(machine_info)

    machine_list = []
    new_machines = []
    for machine_id, value in machine_info.items():
        cpu_idle_rate = float(min(value["cpu"])) / value["total_cpu"]
        if value["total_cpu"] > 50 and cpu_idle_rate > 0.5:
            machine_list.append(machine_id)
            if len(value["insts"]) == 0:
                new_machines.append(machine_id)
    for machine_id, value in machine_info.items():
        cpu_idle_rate = float(min(value["cpu"])) / value["total_cpu"]
        if cpu_idle_rate < 0.75 and cpu_idle_rate > 0.5 and value["total_cpu"] <= 50:
            machine_list.append(machine_id)
    for machine_id, value in machine_info.items():
        cpu_idle_rate = float(min(value["cpu"])) / value["total_cpu"]
        if cpu_idle_rate <= 0.99999999 and cpu_idle_rate >= 0.75 and value["total_cpu"] <= 50:
            machine_list.append(machine_id)
    for machine_id, value in machine_info.items():
        #cpu_idle_rate = float(max(value["cpu"])) / value["total_cpu"]
        if len(value["insts"]) == 0 and value["total_cpu"] <= 50:
            machine_list.append(machine_id)
            new_machines.append(machine_id)
    print len(machine_list)

    allocate_list = []
    deal_with_conflict(machine_info, inst_info, allocate_list, limits, new_machines)
    print "deal with conflict done!"
    deal_with_overload(machine_info, inst_info, allocate_list, limits, machine_list)


    c1 = 0
    c2 = 0
    c3 = 0
    c4 = 0
    c5 = 0
    c6 = 0
    app_colp = Counter(app_id2)
    inst_list = []
    for inst_id, value in inst_info.items():
        if value["allocated"]:
            c1 += 1
            continue
        if max(value["cpus"]) > 10:
            inst_list.append(inst_id)
            c6 += 1

    for inst_id, value in inst_info.items():
        if value["allocated"]:
            continue
        if max(value["cpus"]) > 10:
            continue
        if value["disk"] > 60:
            inst_list.append(inst_id)
            c2 += 1

    for inst_id, value in inst_info.items():
        if value["allocated"]:
            continue
        if max(value["cpus"]) > 10:
            continue
        if value["disk"] > 60:
            continue
        if value["P"] > 0 or value["M"] > 0 or value["PM"] > 0:
            inst_list.append(inst_id)
            c3+=1


    for inst_id, value in inst_info.items():
        if value["allocated"]:
            continue
        if max(value["cpus"]) > 10:
            continue
        if value["disk"] > 60:
            continue
        if value["P"] > 0 or value["M"] > 0 or value["PM"] > 0:
            continue
        if app_colp[value["app_id"]] > 20:
            inst_list.append(inst_id)
            c5 += 1

    for inst_id, value in inst_info.items():
        if value["allocated"]:
            continue
        if max(value["cpus"]) > 10:
            continue
        if value["disk"] > 60:
            continue
        if app_colp[value["app_id"]] > 20:
            continue
        if value["P"] > 0 or value["M"] > 0 or value["PM"] > 0:
            continue
        inst_list.append(inst_id)

        c4 += 1

    print c1, c2, c3, c4, c5, c6, c1+c2+c3+c4+c5+c6

    total_inst = len(inst_info)
    print "allocated:", c1 , "/", total_inst
    c1 = allocate_epoch(machine_list, inst_list, inst_info, machine_info, limits, allocate_list,
                   c1, total_inst, cpu_idle_rate=0.49)

    c1 = allocate_epoch(machine_list, inst_list, inst_info, machine_info, limits, allocate_list,
                   c1, total_inst, cpu_idle_rate=0.45)

    c1 = allocate_epoch(machine_list, inst_list, inst_info, machine_info, limits, allocate_list,
                   c1, total_inst, cpu_idle_rate=0.4)

    c1 = allocate_epoch(machine_list, inst_list, inst_info, machine_info, limits, allocate_list,
                   c1, total_inst, cpu_idle_rate=0.35)

    f = open("result.csv", "w")
    for alloc in allocate_list:
        inst_id, machine_id = alloc
        f.write("inst_"+str(inst_id)+",machine_"+str(machine_id)+"\n")
    f.close()



def main():
    aorb = "a"   # choose dataset a or b
    if aorb == "a":
        app_id1, app_id2, k, app_resources, instances, machines = \
            load_data("scheduling_preliminary_a_app_interference_20180606.csv",
                  "scheduling_preliminary_a_app_resources_20180606.csv",
                  "scheduling_preliminary_a_instance_deploy_20180606.csv",
                  "scheduling_preliminary_a_machine_resources_20180606.csv")
    else:
        app_id1, app_id2, k, app_resources, instances, machines = \
            load_data("scheduling_preliminary_b_app_interference_20180726.csv",
                  "scheduling_preliminary_b_app_resources_20180726.csv",
                  "scheduling_preliminary_b_instance_deploy_20180726.csv",
                  "scheduling_preliminary_b_machine_resources_20180726.csv")

    #exit(0)
    allocate(app_id1, app_id2, k, app_resources, instances, machines)

def concat_ab(file1, file2):
    f = open(file1, "r")
    lines1 = f.readlines()
    f.close()

    f = open(file2, "r")
    lines2 = f.readlines()
    f.close()

    f = open("result_ab.csv", "w")
    for line in lines1:
        f.write(line)
    f.write("#\n")
    for line in lines2:
        f.write(line)
    f.close()


if __name__ == "__main__":
    main()
    #print np.log(1.1)
    #concat_ab("result_a.csv", "result_b.csv")

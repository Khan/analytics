"""This script trains, and then emits features generated using,
a multidimensional item response theory model.

USAGE:

  The KA website root and analytics directory must be on PYTHONPATH, e.g.,

  export PYTHONPATH=~/khan/website/stable:~/khan/analytics/src
  python mirt_train_EM.py -a 1 -n 75 -f PROD_RESPONSES -w 0 -o MIRT_NEW &> LOG

  Where PROD_RESPONSES is a number of UserAssessment data as formatted
  by get_user_assessment_data.py, MIRT_NEW is the root filename for
  output, and LOG is a logfile containing the stderr and stdout of
  this process.

"""

import multiprocessing
from multiprocessing import Pool
import fileinput
import numpy as np
from collections import defaultdict
import copy
import optparse
import sys
import time
import scipy
import scipy.optimize

# the following is imported the Khan web application source
from assessment import mirt_util

import accuracy_model_util as acc_util


# the maximum number of exercises that might exist
MAX_EXERCISES = 10000

# used to index the fields in with a line of text in the input data file
linesplit = acc_util.linesplit
idx_pl = acc_util.FieldIndexer(acc_util.FieldIndexer.plog_fields)


# current_ind and generate_exercise_ind are used in the creation of a
# defaultdict for mapping exercise names to an unique integer index
current_ind = 0


def generate_exercise_ind():
    """Assign the next available index to an exercise name."""
    global current_ind
    current_ind += 1
    return current_ind - 1


def sample_abilities_diffusion(args):
    """Sample the ability vector for this user, from the posterior over user
    ability conditioned on the observed exercise performance.
    use Metropolis-Hastings with Gaussian proposal distribution.

    This is just a wrapper around the corresponding function in mirt_util.
    """

    # make sure each student gets a different random sequence
    id = multiprocessing.current_process()._identity
    if len(id) > 0:
        np.random.seed([id[0], time.time() * 1e9])
    else:
        np.random.seed([time.time() * 1e9])

    couplings, state, options, user_index = args
    abilities = state['abilities']
    correct = state['correct']
    exercises_ind = state['exercises_ind']

    num_steps = options.sampling_num_steps

    abilities, Eabilities, _, _ = mirt_util.sample_abilities_diffusion(
            couplings, exercises_ind, correct, abilities, num_steps)

    return abilities, Eabilities, user_index


def get_cmd_line_options():
    parser = optparse.OptionParser()
    parser.add_option("-a", "--num_abilities", type=int, default=1,
                      help=("Number of hidden ability units"))
    parser.add_option("-s", "--sampling_num_steps", type=int, default=50,
                      help=("Number of sampling steps to use for "
                            "sample_abilities_diffusion"))
    parser.add_option("-l", "--sampling_epsilon", type=float, default=0.1,
                      help=("The length scale to use for sampling update "
                            "proposals"))
    parser.add_option("-n", "--num_epochs", type=int, default=10000,
                      help=("The number of EM iterations to do during "
                            "learning"))
    parser.add_option("-q", "--num_replicas", type=int, default=1,
                      help=("The number of copies of the data to train "
                            "on.  If there is too little training data, "
                            "increase this number in order to maintain "
                            "multiple samples from the abilities vector "
                            "for each student.  A sign that there is too "
                            "little training data is if the update step "
                            "length ||dcouplings|| remains large."))
    parser.add_option("-m", "--max_pass_lbfgs", type=int, default=5,
                      help=("The number of LBFGS descent steps to do per "
                            "EM iteration"))
    parser.add_option("-p", "--regularization", default=1e-5,
                      help=("The weight for an L2 regularizer on the "
                            "parameters.  This can be very small, but "
                            "keeps the weights from running away in a "
                            "weakly constrained direction."))
    parser.add_option("-w", "--workers", type=int, default=6,
                      help=("The number of processes to use to parallelize "
                            "this.  Set this to 0 to use one process, and "
                            "make debugging easier."))
    parser.add_option("-f", "--file", type=str,
                      default='user_assessment.responses',
                      help=("The source data file"))
    parser.add_option("-o", "--output", type=str, default='',
                      help=("The root filename for output"))
    parser.add_option("-t", "--training_set_size", type=float, default=1.0,
                      help=("The fraction (expressed as a number beteween 0.0 "
                            "and 1.0) of the data to be used for training. "
                            "The remainder is held out for testing."))
    parser.add_option("-e", "--emit_features", action="store_true",
                      default=False,
                      help=("Boolean flag indicating whether to output "
                            "feature and prediction data. Often used to "
                            "analyze accuracy of predictions after model "
                            "training."))
    parser.add_option("-r", "--resume_from_file", default='',
                      help=("Name of a .npz file to bootstrap the couplings."))

    options, _ = parser.parse_args()

    if options.output == '':
        # default filename
        options.output = "mirt_file=%s_abilities=%d" % (
                options.file, options.num_abilities)

    return options


def create_user_state(lines, exercise_ind_dict, options):
    """Create a dictionary to hold training information for a single user."""
    correct = np.asarray([line[idx_pl.correct] for line in lines]
            ).astype(int)
    exercises = [line[idx_pl.exercise] for line in lines]
    exercises_ind = [exercise_ind_dict[ex] for ex in exercises]
    exercises_ind = np.array(exercises_ind)
    abilities = np.random.randn(options.num_abilities, 1)
    state = {'correct': correct,
             'abilities': abilities,
             'exercises_ind': exercises_ind}
    return state


def L_dL_singleuser(arg):
    """ calculate log likelihood and gradient wrt couplings of mIRT model
        for single user """
    couplings, state, options = arg
    abilities = state['abilities'].copy()
    correct = state['correct']
    exercises_ind = state['exercises_ind']

    # pad the abilities vector with a 1 to act as a bias
    abilities = np.append(abilities.copy(),
                          np.ones((1, abilities.shape[1])),
                          axis=0)
    # the abilities to exercise coupling parameters for this exercise
    exercise_couplings = couplings[exercises_ind, :]

    # calculate the probability of getting a question in this exercise correct
    Y = np.dot(exercise_couplings, abilities)
    Z = mirt_util.sigmoid(Y)  # predicted correctness value
    Zt = correct.reshape(Z.shape)  # true correctness value
    pdata = Zt * Z + (1 - Zt) * (1 - Z)  # = 2*Zt*Z - Z + const
    dLdY = ((2 * Zt - 1) * Z * (1 - Z)) / pdata
    dL = np.dot(dLdY, abilities.T)
    dL /= np.log(2.)

    L = np.sum(np.log(pdata))
    L /= np.log(2.)

    return -L, -dL, exercises_ind


def L_dL(couplings_flat, user_states, options, pool):
    """ calculate log likelihood and gradient wrt couplings of mIRT model """
    couplings = couplings_flat.copy().reshape((-1, options.num_abilities + 1))

    L = 0.
    dL = np.zeros(couplings.shape)

    # TODO(jascha) this would be faster if user_states was divided into
    # minibatches instead of single students
    if pool is None:
        rslts = map(L_dL_singleuser,
                    [(couplings, state, options) for state in user_states])
    else:
        rslts = pool.map(L_dL_singleuser,
                        [(couplings, state, options) for state in user_states],
                        chunksize=100)
    for r in rslts:
        Lu, dLu, exercise_indu = r
        L += Lu / float(len(user_states))
        dL[exercise_indu, :] += dLu / float(len(user_states))

    L += options.regularization * sum(couplings_flat ** 2)
    dL += 2. * options.regularization * couplings

    return L, dL.ravel()


def emit_features(user_states, couplings, options, split_desc):
    """Emit a CSV data file of correctness, prediction, and abilities."""
    f = open("%s_split=%s.csv" % (options.output, split_desc), 'w+')

    for user_state in user_states:
        # initialize
        abilities = np.zeros((options.num_abilities, 1))
        correct = user_state['correct']
        exercises_ind = user_state['exercises_ind']

        # NOTE: I currently do not output features for the first problem
        for i in xrange(1, correct.size):

            # TODO(jace) this should probably be the marginal estimation
            _, _, abilities, _ = mirt_util.sample_abilities_diffusion(
                    couplings, exercises_ind[:i], correct[:i],
                    abilities_init=abilities, num_steps=200)
            prediction = mirt_util.conditional_probability_correct(
                    abilities, couplings, exercises_ind[i:(i + 1)])

            print >>f, "%d," % correct[i],
            print >>f, "%.4f," % prediction[-1],
            print >>f, ",".join(["%.4f" % a for a in abilities])

    f.close()


def main():
    options = get_cmd_line_options()
    print >>sys.stderr, "Starting main.", options  # DEBUG

    # initialize the parameter matrix to zeros (+1 for bias unit)
    couplings = np.zeros((MAX_EXERCISES, options.num_abilities + 1))

    pool = None
    if options.workers > 0:
        pool = Pool(options.workers)

    exercise_ind_dict = defaultdict(generate_exercise_ind)

    user_states = []
    user_states_train = []
    user_states_test = []

    print >>sys.stderr, "loading data"
    prev_user = None
    attempts = []
    for replica_num in range(options.num_replicas):
        # loop through all the training data, and create user objects
        for line in fileinput.input(options.file):
            # split on either tab or \x01 so the code works via Hive or pipe
            row = linesplit.split(line.strip())

            # TODO(jace): If training on UserAssessment data, the 'user'
            # field here is probably populated with the UserAssessment key.
            # Fix the naming.
            user = row[idx_pl.user]
            if prev_user and user != prev_user and len(attempts) > 0:
                # We're getting a new user, so perform the reduce operation
                # on our previous user
                user_states.append(create_user_state(
                        attempts, exercise_ind_dict, options))
                attempts = []
            prev_user = user
            if row[idx_pl.rowtype] == 'problemlog':
                row[idx_pl.correct] = row[idx_pl.correct] == 'true'
                row[idx_pl.eventually_correct] = (
                    row[idx_pl.eventually_correct] == 'true')
                row[idx_pl.problem_number] = int(row[idx_pl.problem_number])
                row[idx_pl.number_attempts] = int(row[idx_pl.number_attempts])
                row[idx_pl.number_hints] = int(row[idx_pl.number_hints])
                row[idx_pl.time_taken] = float(row[idx_pl.time_taken])
                attempts.append(row)

        if len(attempts) > 0:
            # flush the data for the final user, too
            user_states.append(create_user_state(
                    attempts, exercise_ind_dict, options))
            attempts = []

        fileinput.close()
        # Reset prev_user so we have equal user_states from each replica
        prev_user = None

        # split into training and test
        if options.training_set_size < 1.0:
            training_cutoff = int(len(user_states) * options.training_set_size)
            user_states_train += copy.deepcopy(user_states[:training_cutoff])
            print >>sys.stderr, len(user_states_train)
            if replica_num == 0:
                # we don't replicate the test data (only training data)
                user_states_test = copy.deepcopy(user_states[training_cutoff:])
            user_states = []

    # if splitting data into test/training sets, set user_states to training
    user_states = user_states_train if user_states_train else user_states

    # DEBUG(jace)
    print >>sys.stderr, "loaded %d user assessments" % len(user_states)

    # trim couplings to include only the used rows
    couplings = couplings[:current_ind, :]

    if options.resume_from_file:
        # HACK(jace): I need a cheap way
        # to output features from a previously trained model.  To use this
        # hacky version, pass --num_epochs 0 and you must pass the same
        # data file the model in resume_from_file was trained on.
        resume_from_model = np.load(options.resume_from_file)
        couplings = resume_from_model['couplings']
        exercise_ind_dict = resume_from_model['exercise_ind_dict']
        print >>sys.stderr, "Loaded couplings of shape:" + str(couplings.shape)

    # now do num_epochs EM steps
    for epoch in range(options.num_epochs):
        print >>sys.stderr, "epoch %d, " % epoch,

        # Expectation step
        # Compute (and print) the energies during learning as a diagnostic.
        # These should decrease.
        Eavg = 0.
        # TODO(jascha) this would be faster if user_states was divided into
        # minibatches instead of single students
        if pool is None:
            rslts = map(sample_abilities_diffusion,
                        [(couplings, user_states[ind], options, ind)
                            for ind in range(len(user_states))])
        else:
            rslts = pool.map(sample_abilities_diffusion,
                            [(couplings, user_states[ind], options, ind)
                                for ind in range(len(user_states))],
                            chunksize=100)
        for r in rslts:
            abilities, El, ind = r
            user_states[ind]['abilities'] = abilities.copy()
            Eavg += El / float(len(user_states))
        print >>sys.stderr, "E joint log L + const %f, " % (
                -Eavg / np.log(2.)),

        # debugging info -- accumulate mean and covariance of abilities vector
        mn_a = 0.
        cov_a = 0.
        for state in user_states:
            mn_a += state['abilities'][:, 0].T / float(len(user_states))
            cov_a += (state['abilities'][:, 0] ** 2).T / (
                        float(len(user_states)))
        print >>sys.stderr, "<abilities>", mn_a,
        print >>sys.stderr, ", <abilities^2>", cov_a, ", ",

        # Maximization step
        old_couplings = couplings.copy()
        couplings_flat, L, _ = scipy.optimize.fmin_l_bfgs_b(
            L_dL,
            couplings.copy().ravel(),
            args=(user_states, options, pool),
            disp=0,
            maxfun=options.max_pass_lbfgs, m=100)
        couplings = couplings_flat.reshape(couplings.shape)

        # Print debugging info on the progress of the training
        print >>sys.stderr, "M conditional log L %f, " % (-L),
        print >>sys.stderr, "reg penalty %f, " % (
                options.regularization * sum(couplings_flat ** 2)),
        print >>sys.stderr, "||couplings|| %f, " % (
                np.sqrt(np.sum(couplings ** 2))),
        print >>sys.stderr, "||dcouplings|| %f" % (
                np.sqrt(np.sum((couplings - old_couplings) ** 2)))

        # Maintain a consistent directional meaning of a
        # high/low ability esimtate.  We always prefer higher ability to
        # mean better performance; therefore, we prefer positive couplings.
        # So, compute the sign of the average coupling for each dimension.
        coupling_sign = np.sign(np.mean(couplings[:, :-1], axis=0))
        coupling_sign = coupling_sign.reshape((1, -1))
        # Then, flip ability and coupling sign for dimenions w/ negative mean.
        couplings[:, :-1] *= coupling_sign
        for user_state in user_states:
            user_state['abilities'] *= coupling_sign

        # save state as a .npz
        np.savez("%s_epoch=%d.npz" % (options.output, epoch),
                 couplings=couplings,
                 exercise_ind_dict=dict(exercise_ind_dict))

        # save state as .csv - just for easy debugging inspection
        f1 = open("%s_epoch=%d.csv" % (options.output, epoch), 'w+')
        tt = [(couplings[exercise_ind_dict[nm], :-1],
                couplings[exercise_ind_dict[nm], -1], nm)
                for nm in exercise_ind_dict.keys()]
        tt = sorted(tt, key=lambda tl: tl[1])
        print >>f1, 'bias, ',
        for ii in range(tt[0][0].shape[0]):
            print >>f1, "coupling %d, " % ii,
        print >>f1, 'exercise name'
        for t in tt:
            print >>f1, t[1], ',',
            for ii in range(tt[0][0].shape[0]):
                print >>f1, t[0][ii], ',',
            print >>f1, t[2]
        f1.close()

    if options.emit_features:
        if options.training_set_size < 1.0:
            emit_features(user_states_train, couplings, options, "train")
            emit_features(user_states_test, couplings, options, "test")
        else:
            emit_features(user_states, couplings, options, "full")


if __name__ == '__main__':
    main()
